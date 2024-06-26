import re
import sys
from typing import TYPE_CHECKING, Dict, List, Optional, Pattern

import humanize
from aiohttp import ClientResponseError
from aiohttp.web_exceptions import HTTPNotFound
from rich import print as rprint
from rich.table import Column, Table
from structlog.stdlib import BoundLogger

from backy.repository import StatusDict
from backy.utils import format_datetime_local

if TYPE_CHECKING:
    from backy.daemon import BackyDaemon


# XXX this is partially duplicated in the daemon
def status(self, filter_re: Optional[Pattern[str]] = None) -> List[StatusDict]:
    """Collects status information for all jobs."""
    # XXX with a database backend, we can evaluate this in live actually
    # so this should move to the CLI client
    result: List["BackyDaemon.StatusDict"] = []
    for job in list(self.jobs.values()):
        if filter_re and not filter_re.search(job.name):
            continue
        job.backup.scan()
        manual_tags = set()
        unsynced_revs = 0
        history = job.backup.clean_history
        for rev in history:
            manual_tags |= filter_manual_tags(rev.tags)
            if rev.pending_changes:
                unsynced_revs += 1
        result.append(
            dict(
                job=job.name,
                sla="OK" if job.sla else "TOO OLD",
                sla_overdue=job.sla_overdue,
                status=job.status,
                last_time=history[-1].timestamp if history else None,
                last_tags=(
                    ",".join(job.schedule.sorted_tags(history[-1].tags))
                    if history
                    else None
                ),
                last_duration=(
                    history[-1].stats.get("duration", 0) if history else None
                ),
                next_time=job.next_time,
                next_tags=(
                    ",".join(job.schedule.sorted_tags(job.next_tags))
                    if job.next_tags
                    else None
                ),
                manual_tags=", ".join(manual_tags),
                quarantine_reports=len(job.backup.quarantine.report_ids),
                unsynced_revs=unsynced_revs,
                local_revs=len(job.backup.get_history(clean=True, local=True)),
            )
        )
    return result


class CLIClient:
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
