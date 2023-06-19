import datetime
import re
from json import JSONEncoder
from typing import Any, List, Tuple

from aiohttp import hdrs, web
from aiohttp.web_exceptions import HTTPAccepted, HTTPNotFound, HTTPUnauthorized
from aiohttp.web_middlewares import middleware
from aiohttp.web_runner import AppRunner, TCPSite
from structlog.stdlib import BoundLogger

import backy.daemon


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
        self.log = log.bind(subsystem="api")
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
            path=request.path, query=request.query_string
        )
        request["log"].debug("new-conn")
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
        request["log"] = request["log"].bind(client=client)
        request["log"].debug("auth-passed")
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

    async def get_status(self, request: web.Request):
        filter = request.query.get("filter", "")
        if filter:
            filter = re.compile(filter)
        return self.daemon.status(filter)

    async def reload_daemon(self, request: web.Request):
        self.daemon.reload()

    async def get_jobs(self, request: web.Request):
        return list(self.daemon.jobs.values())

    async def get_job(self, request: web.Request):
        try:
            name = request.match_info.get("job_name", None)
            return self.daemon.jobs[name]
        except KeyError:
            raise HTTPNotFound()

    async def run_job(self, request: web.Request):
        j = await self.get_job(request)
        j.run_immediately.set()
        raise HTTPAccepted()
