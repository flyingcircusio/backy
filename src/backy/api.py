import datetime
import re
from json import JSONEncoder
from pathlib import Path
from typing import Any, List, Tuple

from aiohttp import hdrs, web
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

import backy.daemon
from backy.backup import Backup
from backy.revision import Revision
from backy.scheduler import Job
from backy.utils import generate_taskid


class BackyJSONEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if hasattr(o, "to_dict"):
            return o.to_dict()
        elif isinstance(o, datetime.datetime):
            return o.isoformat()
        else:
            super().default(o)


class BackyAPI:
    daemon: "backy.daemon.BackyDaemon"
    sites: dict[Tuple[str, int], TCPSite]
    runner: AppRunner
    tokens: dict
    log: BoundLogger

    def __init__(self, daemon, log):
        self.log = log.bind(subsystem="api", job_name="~")
        self.daemon = daemon
        self.sites = {}
        self.app = web.Application(
            middlewares=[self.log_conn, self.require_auth, self.to_json]
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
        for bind_addr, site in self.sites.items():
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

    @middleware
    async def to_json(self, request: web.Request, handler):
        resp = await handler(request)
        if isinstance(resp, web.Response):
            return resp
        elif resp is None:
            raise web.HTTPNoContent()
        else:
            return web.json_response(resp, dumps=BackyJSONEncoder().encode)

    async def get_status(self, request: web.Request) -> List[dict]:
        filter = request.query.get("filter", None)
        request["log"].info("get-status", filter=filter)
        if filter:
            filter = re.compile(filter)
        return self.daemon.status(filter)

    async def reload_daemon(self, request: web.Request):
        request["log"].info("reload-daemon")
        self.daemon.reload()

    async def get_jobs(self, request: web.Request) -> List[Job]:
        request["log"].info("get-jobs")
        return list(self.daemon.jobs.values())

    async def get_job(self, request: web.Request) -> Job:
        name = request.match_info.get("job_name", None)
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

    async def list_backups(self, request: web.Request) -> List[str]:
        request["log"].info("list-backups")
        return await self.daemon.find_dead_backups()

    async def get_backup(self, request: web.Request) -> Backup:
        name = request.match_info.get("backup_name", None)
        request["log"].info("get-backups", name=name)
        if name in self.daemon.jobs:
            request["log"].info("get-backups-forbidden", name=name)
            raise HTTPForbidden()
        try:
            path = Path(self.daemon.base_dir).joinpath(name).resolve()
            if (
                not path.exists()
                or Path(self.daemon.base_dir).resolve() not in path.parents
            ):
                raise FileNotFoundError
            return Backup(path, request["log"])
        except FileNotFoundError:
            request["log"].info("get-backups-not-found", name=name)
            raise HTTPNotFound()

    async def run_purge(self, request: web.Request):
        backup = await self.get_backup(request)
        request["log"].info("run-purge", name=backup.name)
        backup.set_purge_pending()
        raise HTTPAccepted()

    async def touch_backup(self, request: web.Request):
        backup = await self.get_backup(request)
        request["log"].info("touch-backup", name=backup.name)
        backup.touch()

    async def get_revs(self, request: web.Request) -> List[Revision]:
        backup = await self.get_backup(request)
        request["log"].info("get-revs", name=backup.name)
        return backup.get_history(
            local=True, clean=request.query.get("only_clean", "") == "1"
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
        spec = request.match_info.get("rev_spec", None)
        backup = await self.get_backup(request)
        request["log"].info(
            "put-tags",
            name=backup.name,
            old_tags=old_tags,
            new_tags=new_tags,
            spec=spec,
            autoremove=autoremove,
        )
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
