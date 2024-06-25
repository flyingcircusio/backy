import datetime
import re
from asyncio import get_running_loop
from json import JSONEncoder
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Tuple

import aiohttp
from aiohttp import ClientTimeout, TCPConnector, hdrs, web
from aiohttp.web_exceptions import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPForbidden,
    HTTPNotFound,
    HTTPPreconditionFailed,
    HTTPServiceUnavailable,
    HTTPUnauthorized,
)
from aiohttp.web_middlewares import middleware
from aiohttp.web_runner import AppRunner, TCPSite
from structlog.stdlib import BoundLogger

import backy.backup
from backy.backup import Backup, StatusDict
from backy.revision import Revision
from backy.utils import generate_taskid

if TYPE_CHECKING:
    from backy.daemon import BackyDaemon

    from .scheduler import Job


class BackyJSONEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if hasattr(o, "to_dict"):
            return o.to_dict()
        elif isinstance(o, datetime.datetime):
            return o.isoformat()
        else:
            super().default(o)


def to_json(response: Any) -> aiohttp.web.StreamResponse:
    if response is None:
        raise web.HTTPNoContent()
    return web.json_response(response, dumps=BackyJSONEncoder().encode)


class BackyAPI:
    daemon: "BackyDaemon"
    sites: dict[Tuple[str, int], TCPSite]
    runner: AppRunner
    tokens: dict
    log: BoundLogger

    def __init__(self, daemon, log):
        self.log = log.bind(subsystem="api", job_name="~")
        self.daemon = daemon
        self.sites = {}
        self.app = web.Application(
            middlewares=[self.log_conn, self.require_auth]
        )
        self.app.add_routes(
            [
                web.get("/v1/status", self.get_status),
                web.post("/v1/reload", self.reload_daemon),
                web.get("/v1/jobs", self.get_jobs),
                web.post("/v1/jobs/{job_name}/run", self.run_job),
                web.get("/v1/backups", self.list_backups),
                web.post("/v1/backups/{backup_name}/purge", self.run_purge),
                web.post("/v1/backups/{backup_name}/touch", self.touch_backup),
                web.get("/v1/backups/{backup_name}/revs", self.get_revs),
                web.put(
                    "/v1/backups/{backup_name}/revs/{rev_spec}/tags",
                    self.put_tags,
                ),
            ]
        )

    async def start(self):
        self.runner = AppRunner(self.app)
        await self.runner.setup()

    async def stop(self):
        await self.runner.cleanup()
        self.sites = {}

    async def reconfigure(
        self, tokens: dict[str, str], addrs: List[str], port: int
    ):
        self.log.debug("reconfigure")
        self.tokens = tokens
        bind_addrs = [(addr, port) for addr in addrs if addr and port]
        for bind_addr in bind_addrs:
            if bind_addr in self.sites:
                continue
            self.sites[bind_addr] = site = TCPSite(
                self.runner, bind_addr[0], bind_addr[1]
            )
            await site.start()
            self.log.info("added-site", site=site.name)
        for bind_addr, site in list(self.sites.items()):
            if bind_addr in bind_addrs:
                continue
            await site.stop()
            del self.sites[bind_addr]
            self.log.info("deleted-site", site=site.name)

    @middleware
    async def log_conn(self, request: web.Request, handler):
        request["log"] = self.log.bind(
            sub_taskid=request.headers.get("taskid"),
            taskid=generate_taskid(),
        )
        request["log"].debug(
            "new-conn", path=request.path, query=request.query_string
        )
        try:
            resp = await handler(request)
        except Exception as e:
            if not isinstance(e, web.HTTPException):
                request["log"].exception("error-handling-request")
            else:
                request["log"].debug(
                    "request-result", status_code=e.status_code
                )
            raise
        request["log"].debug(
            "request-result", status_code=resp.status, response=resp.body
        )
        return resp

    @middleware
    async def require_auth(self, request: web.Request, handler):
        token = request.headers.get(hdrs.AUTHORIZATION, "")
        if not token.startswith("Bearer "):
            request["log"].info("auth-invalid-token")
            raise HTTPUnauthorized()
        token = token.removeprefix("Bearer ")
        client = self.tokens.get(token, None)
        if not client:
            request["log"].info("auth-token-unknown")
            raise HTTPUnauthorized()
        request["client"] = client
        request["log"] = request["log"].bind(job_name="~" + client)
        return await handler(request)

    async def get_status(
        self, request: web.Request
    ) -> aiohttp.web.StreamResponse:
        filter = request.query.get("filter", None)
        request["log"].info("get-status", filter=filter)
        filter_re = re.compile(filter) if filter else None
        return to_json(self.daemon.status(filter_re))

    async def reload_daemon(self, request: web.Request):
        request["log"].info("reload-daemon")
        self.daemon.reload()
        return to_json(None)

    async def get_jobs(self, request: web.Request):
        request["log"].info("get-jobs")
        return to_json(list(self.daemon.jobs.values()))

    async def get_job(self, request: web.Request) -> "Job":
        name = request.match_info.get("job_name")
        if name is None:
            request["log"].info("empty-job")
            raise HTTPNotFound()

        request["log"].info("get-job", name=name)
        try:
            return self.daemon.jobs[name]
        except KeyError:
            request["log"].info("get-job-not-found", name=name)
            raise HTTPNotFound()

    async def run_job(self, request: web.Request):
        j = await self.get_job(request)
        request["log"].info("run-job", name=j.name)
        j.run_immediately.set()
        raise HTTPAccepted()

    async def list_backups(self, request: web.Request):
        request["log"].info("list-backups")
        return to_json(list(self.daemon.dead_backups.keys()))

    async def get_backup(
        self, request: web.Request, allow_active: bool
    ) -> Backup:
        name = request.match_info.get("backup_name")
        request["log"].info("get-backups", name=name)
        if name in self.daemon.dead_backups:
            return self.daemon.dead_backups[name]
        if name in self.daemon.jobs:
            if allow_active:
                return self.daemon.jobs[name].backup
            request["log"].info("get-backups-forbidden", name=name)
            raise HTTPForbidden()
        request["log"].info("get-backups-not-found", name=name)
        raise HTTPNotFound()

    async def run_purge(self, request: web.Request):
        backup = await self.get_backup(request, False)
        request["log"].info("run-purge", name=backup.name)
        backup.set_purge_pending()
        raise HTTPAccepted()

    async def touch_backup(self, request: web.Request):
        backup = await self.get_backup(request, True)
        request["log"].info("touch-backup", name=backup.name)
        backup.touch()
        raise web.HTTPNoContent()

    async def get_revs(self, request: web.Request):
        backup = await self.get_backup(request, True)
        request["log"].info("get-revs", name=backup.name)
        backup.scan()
        return to_json(
            backup.get_history(
                local=True, clean=request.query.get("only_clean", "") == "1"
            )
        )

    async def put_tags(self, request: web.Request):
        json = await request.json()
        try:
            old_tags = set(json["old_tags"])
            new_tags = set(json["new_tags"])
        except KeyError:
            request["log"].info("put-tags-bad-request")
            raise HTTPBadRequest()
        autoremove = request.query.get("autoremove", "") == "1"
        spec = request.match_info.get("rev_spec")
        backup = await self.get_backup(request, False)
        request["log"].info(
            "put-tags",
            name=backup.name,
            old_tags=old_tags,
            new_tags=new_tags,
            spec=spec,
            autoremove=autoremove,
        )
        backup.scan()
        try:
            if not backup.tags(
                "set",
                spec,
                new_tags,
                old_tags,
                autoremove=autoremove,
                force=True,
            ):
                raise HTTPPreconditionFailed()
        except KeyError:
            request["log"].info("put-tags-rev-not-found")
            raise HTTPNotFound()
        except BlockingIOError:
            request["log"].info("put-tags-locked")
            raise HTTPServiceUnavailable()
        raise web.HTTPNoContent()


class ClientManager:
    connector: TCPConnector
    peers: dict[str, dict]
    clients: dict[str, "Client"]
    taskid: str
    log: BoundLogger

    def __init__(self, peers: Dict[str, dict], taskid: str, log: BoundLogger):
        self.connector = TCPConnector()
        self.peers = peers
        self.clients = dict()
        self.taskid = taskid
        self.log = log.bind(subsystem="ClientManager")

    def __getitem__(self, name: str) -> "Client":
        if name and name not in self.clients:
            self.clients[name] = Client.from_conf(
                name, self.peers[name], self.taskid, self.log, self.connector
            )
        return self.clients[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self.peers)

    async def close(self) -> None:
        for c in self.clients.values():
            await c.close()
        await self.connector.close()

    async def __aenter__(self) -> "ClientManager":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class Client:
    log: BoundLogger
    server_name: str
    session: aiohttp.ClientSession

    def __init__(
        self,
        server_name: str,
        url: str,
        token: str,
        taskid: str,
        log,
        connector=None,
    ):
        assert get_running_loop().is_running()
        self.log = log.bind(subsystem="APIClient")
        self.server_name = server_name
        self.session = aiohttp.ClientSession(
            url,
            headers={hdrs.AUTHORIZATION: "Bearer " + token, "taskid": taskid},
            raise_for_status=True,
            timeout=ClientTimeout(30, connect=10),
            connector=connector,
            connector_owner=connector is None,
        )

    @classmethod
    def from_conf(cls, server_name, conf, *args, **kwargs):
        return cls(
            server_name,
            conf["url"],
            conf["token"],
            *args,
            **kwargs,
        )

    async def fetch_status(self, filter: str = "") -> List[StatusDict]:
        async with self.session.get(
            "/v1/status", params={"filter": filter}
        ) as response:
            jobs = await response.json()
            for job in jobs:
                if job["last_time"]:
                    job["last_time"] = datetime.datetime.fromisoformat(
                        job["last_time"]
                    )
                if job["next_time"]:
                    job["next_time"] = datetime.datetime.fromisoformat(
                        job["next_time"]
                    )
            return jobs

    async def reload_daemon(self):
        async with self.session.post("/v1/reload"):
            return

    async def get_jobs(self) -> List[dict]:
        async with self.session.get("/v1/jobs") as response:
            return await response.json()

    async def run_job(self, name: str):
        async with self.session.post(f"/v1/jobs/{name}/run"):
            return

    async def list_backups(self) -> List[str]:
        async with self.session.get("/v1/backups") as response:
            return await response.json()

    async def run_purge(self, name: str):
        async with self.session.post(f"/v1/backups/{name}/purge"):
            return

    async def touch_backup(self, name: str):
        async with self.session.post(f"/v1/backups/{name}/touch"):
            return

    async def get_revs(
        self, backup: "backy.backup.Backup", only_clean: bool = True
    ) -> List[Revision]:
        async with self.session.get(
            f"/v1/backups/{backup.name}/revs",
            params={"only_clean": int(only_clean)},
        ) as response:
            json = await response.json()
            revs = [Revision.from_dict(r, backup, self.log) for r in json]
            for r in revs:
                r.orig_tags = r.tags
                r.server = self.server_name
            return revs

    async def put_tags(self, rev: Revision, autoremove: bool = False):
        async with self.session.put(
            f"/v1/backups/{rev.backup.name}/revs/{rev.uuid}/tags",
            json={"old_tags": list(rev.orig_tags), "new_tags": list(rev.tags)},
            params={"autoremove": int(autoremove)},
        ):
            return

    async def close(self):
        await self.session.close()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
