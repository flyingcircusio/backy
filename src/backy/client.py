import datetime
import re
import sys
from asyncio import get_running_loop
from typing import TYPE_CHECKING, Dict, Iterator, List

import aiohttp
import humanize
from aiohttp import ClientResponseError, ClientTimeout, TCPConnector, hdrs
from aiohttp.web_exceptions import HTTPNotFound
from rich import print as rprint
from rich.table import Column, Table
from structlog.stdlib import BoundLogger

import backy.backup
from backy.revision import Revision
from backy.utils import format_datetime_local

if TYPE_CHECKING:
    from backy.daemon import BackyDaemon


class APIClientManager:
    connector: TCPConnector
    peers: dict[str, dict]
    clients: dict[str, "APIClient"]
    taskid: str
    log: BoundLogger

    def __init__(self, peers: Dict[str, dict], taskid: str, log: BoundLogger):
        self.connector = TCPConnector()
        self.peers = peers
        self.clients = dict()
        self.taskid = taskid
        self.log = log.bind(subsystem="APIClientManager")

    def __getitem__(self, name: str) -> "APIClient":
        if name and name not in self.clients:
            self.clients[name] = APIClient.from_conf(
                name, self.peers[name], self.taskid, self.log, self.connector
            )
        return self.clients[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self.peers)

    async def close(self) -> None:
        for c in self.clients.values():
            await c.close()
        await self.connector.close()

    async def __aenter__(self) -> "APIClientManager":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class APIClient:
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

    async def fetch_status(
        self, filter: str = ""
    ) -> List["BackyDaemon.StatusDict"]:
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
        async with self.session.post(f"/v1/reload") as response:
            return

    async def get_jobs(self) -> List[dict]:
        async with self.session.get("/v1/jobs") as response:
            return await response.json()

    async def run_job(self, name: str):
        async with self.session.post(f"/v1/jobs/{name}/run") as response:
            return

    async def list_backups(self) -> List[str]:
        async with self.session.get("/v1/backups") as response:
            return await response.json()

    async def run_purge(self, name: str):
        async with self.session.post(f"/v1/backups/{name}/purge") as response:
            return

    async def touch_backup(self, name: str):
        async with self.session.post(f"/v1/backups/{name}/touch") as response:
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
                r.backend_type = ""
                r.orig_tags = r.tags
                r.server = self.server_name
            return revs

    async def put_tags(self, rev: Revision, autoremove: bool = False):
        async with self.session.put(
            f"/v1/backups/{rev.backup.name}/revs/{rev.uuid}/tags",
            json={"old_tags": list(rev.orig_tags), "new_tags": list(rev.tags)},
            params={"autoremove": int(autoremove)},
        ) as response:
            return

    async def close(self):
        await self.session.close()

    async def __aenter__(self) -> "APIClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class CLIClient:
    api: APIClient
    log: BoundLogger

    def __init__(self, apiclient, log):
        self.api = apiclient
        self.log = log.bind(subsystem="CLIClient")

    async def __aenter__(self) -> "CLIClient":
        await self.api.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.api.__aexit__(exc_type, exc_val, exc_tb)

    async def jobs(self, filter_re=""):
        """List status of all known jobs. Optionally filter by regex."""

        tz = format_datetime_local(None)[1]

        t = Table(
            "Job",
            "SLA",
            "SLA overdue",
            "Status",
            f"Last Backup ({tz})",
            "Last Tags",
            Column("Last Duration", justify="right"),
            f"Next Backup ({tz})",
            "Next Tags",
        )

        jobs = await self.api.fetch_status(filter_re)
        jobs.sort(key=lambda j: j["job"])
        for job in jobs:
            overdue = (
                humanize.naturaldelta(job["sla_overdue"])
                if job["sla_overdue"]
                else "-"
            )
            last_duration = (
                humanize.naturaldelta(job["last_duration"])
                if job["last_duration"]
                else "-"
            )
            last_time = format_datetime_local(job["last_time"])[0]
            next_time = format_datetime_local(job["next_time"])[0]

            t.add_row(
                job["job"],
                job["sla"],
                overdue,
                job["status"],
                last_time,
                job["last_tags"],
                last_duration,
                next_time,
                job["next_tags"],
            )
        backups = await self.api.list_backups()
        if filter_re:
            backups = list(filter(re.compile(filter_re).search, backups))
        for b in backups:
            t.add_row(b, "-", "-", "Dead", "-", "", "-", "-", "")

        rprint(t)
        print("{} jobs shown".format(len(jobs) + len(backups)))

    async def status(self):
        """Show job status overview"""
        t = Table("Status", "#")
        state_summary: Dict[str, int] = {}
        jobs = await self.api.get_jobs()
        jobs += [{"status": "Dead"} for _ in await self.api.list_backups()]
        for job in jobs:
            state_summary.setdefault(job["status"], 0)
            state_summary[job["status"]] += 1

        for state in sorted(state_summary):
            t.add_row(state, str(state_summary[state]))
        rprint(t)

    async def run(self, job: str):
        """Trigger immediate run for one job"""
        try:
            await self.api.run_job(job)
        except ClientResponseError as e:
            if e.status == HTTPNotFound.status_code:
                self.log.error("unknown-job", job=job)
                sys.exit(1)
            raise
        self.log.info("triggered-run", job=job)

    async def runall(self):
        """Trigger immediate run for all jobs"""
        jobs = await self.api.get_jobs()
        for job in jobs:
            await self.run(job["name"])

    async def reload(self):
        """Reload the configuration."""
        self.log.info("reloading-daemon")
        await self.api.reload_daemon()
        self.log.info("reloaded-daemon")

    async def check(self):
        status = await self.api.fetch_status()

        exitcode = 0

        for job in status:
            log = self.log.bind(job_name=job["job"])
            if job["manual_tags"]:
                log.info(
                    "check-manual-tags",
                    manual_tags=job["manual_tags"],
                )
            if job["unsynced_revs"]:
                self.log.info(
                    "check-unsynced-revs", unsynced_revs=job["unsynced_revs"]
                )
            if job["sla"] != "OK":
                log.critical(
                    "check-sla-violation",
                    last_time=str(job["last_time"]),
                    sla_overdue=job["sla_overdue"],
                )
                exitcode = max(exitcode, 2)
            if job["quarantine_reports"]:
                log.warning(
                    "check-quarantined", reports=job["quarantine_reports"]
                )
                exitcode = max(exitcode, 1)

        self.log.info("check-exit", exitcode=exitcode, jobs=len(status))
        raise SystemExit(exitcode)
