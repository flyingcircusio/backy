import argparse
import asyncio
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import humanize
import structlog
import tzlocal
import yaml
from aiohttp import ClientResponseError
from aiohttp.web_exceptions import HTTPNotFound
from rich import print as rprint
from rich.table import Column, Table
from structlog.stdlib import BoundLogger

import backy.daemon
import backy.source
from backy import logging
from backy.daemon import BackyDaemon
from backy.daemon.api import Client
from backy.rbd import RestoreBackend

# XXX invert this dependency
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule
from backy.utils import format_datetime_local, generate_taskid

# single repo commands


# (init)

# rev-parse                Print full path or uuid of specified revisions

# log [--filter] (status)              Show backup status. Show inventory and summary information

# backup [--fg] (remote)            Perform a backup
# restore (remote)            Restore (a given revision) to a given target

# distrust            Distrust specified revisions
# verify (remote)             Verify specified revisions
# rm                  Forget specified revision
# tag                 Modify tags on revision

# gc [--expire] [--remote|--local]       (Expire revisions) and collect garbage from the repository.

# pull?               update metadata from all known remotes that host backups
#                     for the same backup source

# check


# # multi-repo / daemon-based commands

# show-jobs           List status of all known jobs
# show-daemon         Daemon status

# pull

# backup --all [--include=filter] [--exclude=filter] [--fg]

# check --all


# maybe add a common --repo/--job <regex> flag?


class Command(object):
    """Proxy between CLI calls and actual backup code."""

    path: Path
    config: Path
    taskid: str
    log: BoundLogger

    def __init__(self, path: Path, config: Path, taskid, log: BoundLogger):
        self.path = path.resolve()
        self.config = config
        self.taskid = taskid
        self.log = log

    def __call__(self, cmdname: str, args: dict[str, Any]):
        func = getattr(self, cmdname)
        ret = func(**args)
        if not isinstance(ret, int):
            ret = 0
        self.log.debug("return-code", code=ret)
        return ret

    def create_api_client(self):
        d = BackyDaemon(self.config, self.log)
        d._read_config()
        return Client.from_conf(
            "<server>", d.api_cli_default, self.taskid, self.log
        )

    def init(self, type):
        sourcefactory = backy.source.factory_by_type(type)
        source = sourcefactory(*sourcefactory.argparse())
        # TODO: check if repo already exists
        repo = Repository(self.path / "config", source, Schedule(), self.log)
        repo.connect()
        repo.store()

    def rev_parse(self, revision: str, uuid: bool) -> None:
        b = Repository.load(self.path, self.log)
        b.connect()
        for r in b.find_revisions(revision):
            if uuid:
                print(r.uuid)
            else:
                print(r.info_filename)

    def log_(self, yaml_: bool, revision: str) -> None:
        revs = Repository(self.path, self.log).find_revisions(revision)
        if yaml_:
            print(yaml.safe_dump([r.to_dict() for r in revs]))
            return
        total_bytes = 0

        tz = tzlocal.get_localzone()
        t = Table(
            f"Date ({tz})",
            "ID",
            Column("Size", justify="right"),
            Column("Duration", justify="right"),
            "Tags",
            "Trust",
            "Server",
        )

        for r in revs:
            total_bytes += r.stats.get("bytes_written", 0)
            duration = r.stats.get("duration")
            if duration:
                duration = humanize.naturaldelta(duration)
            else:
                duration = "-"

            if r.pending_changes:
                added = [f"+[on green]{t}[/]" for t in r.tags - r.orig_tags]
                removed = [f"-[on red]{t}[/]" for t in r.orig_tags - r.tags]
                same = list(r.orig_tags & r.tags)
                tags = ",".join(added + removed + same)
            else:
                tags = ",".join(r.tags)

            t.add_row(
                format_datetime_local(r.timestamp)[0],
                r.uuid,
                humanize.naturalsize(
                    r.stats.get("bytes_written", 0), binary=True
                ),
                duration,
                tags,
                r.trust.value,
                f"[underline italic]{r.server}[/]"
                if r.pending_changes
                else r.server,
            )

        rprint(t)

        print(
            "{} revisions containing {} data (estimated)".format(
                len(revs), humanize.naturalsize(total_bytes, binary=True)
            )
        )
        pending_changes = sum(1 for r in revs if r.pending_changes)
        if pending_changes:
            rprint(
                f"[yellow]{pending_changes} pending change(s)[/] "
                "(Push changes with `backy push`)"
            )

    def backup(self, tags: str, force: bool) -> int:
        b = Repository(self.path, self.log)
        b._clean()
        tags_ = set(t.strip() for t in tags.split(","))
        if not force:
            b.validate_tags(tags_)
        r = Revision.create(b, tags_, self.log)
        r.materialize()
        proc = subprocess.run(
            [
                b.type.value,
                "-t",
                self.taskid,
                "-b",
                str(self.path),
                "backup",
                r.uuid,
            ],
        )
        b.scan()
        b._clean()
        return proc.returncode

    def restore(
        self, revision: str, target: str, restore_backend: RestoreBackend
    ) -> int:
        b = Repository(self.path, self.log)
        r = b.find(revision)
        proc = subprocess.run(
            [
                b.type.value,
                "-t",
                self.taskid,
                "-b",
                str(self.path),
                "restore",
                "--backend",
                restore_backend.value,
                r.uuid,
                target,
            ]
        )
        return proc.returncode

    def distrust(self, revision: str) -> None:
        b = Repository(self.path, self.log)
        b.distrust(revision)

    def verify(self, revision: str) -> int:
        b = Repository(self.path, self.log)
        r = b.find(revision)
        proc = subprocess.run(
            [
                b.type.value,
                "-t",
                self.taskid,
                "-b",
                str(self.path),
                "verify",
                r.uuid,
            ]
        )
        return proc.returncode

    def rm(self, revision: str) -> None:
        b = Repository(self.path, self.log)
        b.forget(revision)

    def tags(
        self,
        action: Literal["set", "add", "remove"],
        autoremove: bool,
        expect: Optional[str],
        revision: str,
        tags: str,
        force: bool,
    ) -> int:
        tags_ = set(t.strip() for t in tags.split(","))
        if expect is None:
            expect_ = None
        else:
            expect_ = set(t.strip() for t in expect.split(","))
        b = backy.repository.Repository(self.path, self.log)
        success = b.tags(
            action,
            revision,
            tags_,
            expect=expect_,
            autoremove=autoremove,
            force=force,
        )
        return int(not success)

    def gc(self, expire: bool) -> None:
        # XXX needs to update from remote API peers first (pull)
        b = Repository(self.path, self.log)
        if expire:
            b.expire()
        proc = subprocess.run(
            [
                b.type.value,
                "-t",
                self.taskid,
                "-b",
                str(self.path),
                "gc",
            ]
        )
        # if remote:
        # push

    def pull(self):
        pass

    def check(self):
        pass

    def show_jobs(self, filter_re="") -> int:
        """List status of all known jobs. Optionally filter by regex."""
        api = self.create_api_client()

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

        jobs = asyncio.run(api.fetch_status(filter_re))
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
        backups = asyncio.run(api.list_backups())
        if filter_re:
            backups = list(filter(re.compile(filter_re).search, backups))
        for b in backups:
            t.add_row(b, "-", "-", "Dead", "-", "", "-", "-", "")

        rprint(t)
        print("{} jobs shown".format(len(jobs) + len(backups)))

    def show_daemon(self):
        """Show job status overview"""
        api = self.create_api_client()
        t = Table("Status", "#")
        state_summary: Dict[str, int] = {}
        jobs = asyncio.run(api.get_jobs())
        jobs += [{"status": "Dead"} for _ in asyncio.run(api.list_backups())]
        for job in jobs:
            state_summary.setdefault(job["status"], 0)
            state_summary[job["status"]] += 1

        for state in sorted(state_summary):
            t.add_row(state, str(state_summary[state]))
        rprint(t)

    def run(self, job: str):
        """Trigger immediate run for one job"""
        try:
            self.api.run_job(job)
        except ClientResponseError as e:
            if e.status == HTTPNotFound.status_code:
                self.log.error("unknown-job", job=job)
                sys.exit(1)
            raise
        self.log.info("triggered-run", job=job)

    def runall(self):
        """Trigger immediate run for all jobs"""
        jobs = self.api.get_jobs()
        for job in jobs:
            self.run(job["name"])

    def reload(self):
        """Reload the configuration."""
        self.log.info("reloading-daemon")
        self.api.reload_daemon()
        self.log.info("reloaded-daemon")

    def check(self):
        status = self.api.fetch_status()

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


def main():
    parser = argparse.ArgumentParser(
        description="Backy command line client.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="verbose output"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default="/etc/backy.conf",
        help="(default: %(default)s)",
    )
    parser.add_argument(
        "-C",
        default=".",
        type=Path,
        help=(
            "Run as if backy was started in <path> instead of the current "
            "working directory."
        ),
    )

    subparsers = parser.add_subparsers()

    p = subparsers.add_parser("init", help="Create an empty backy repository.")
    p.add_argument(
        "type",
        choices=backy.source.SOURCE_PLUGINS.names,
        help="Type of the source.",
    )
    p.set_defaults(func="init")

    p = subparsers.add_parser("show-jobs", help="List status of all known jobs")
    p.add_argument(
        "filter_re",
        default="",
        metavar="[filter]",
        nargs="?",
        help="Optional job filter regex",
    )
    p.set_defaults(func="show_jobs")

    p = subparsers.add_parser("show-daemon", help="Show job status overview")
    p.set_defaults(func="show_daemon")

    p = subparsers.add_parser(
        "check",
        help="Check whether all jobs adhere to their schedules' SLA",
    )
    p.set_defaults(func="check")

    p = subparsers.add_parser(
        "backup",
        help="Perform a backup",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Do not validate tags"
    )
    p.add_argument("tags", help="Tags to apply to the backup")
    p.set_defaults(func="backup")

    p = subparsers.add_parser(
        "restore",
        help="Restore (a given revision) to a given target",
    )
    p.add_argument(
        "--backend",
        type=RestoreBackend,
        choices=list(RestoreBackend),
        default=RestoreBackend.AUTO,
        dest="restore_backend",
        help="(default: %(default)s)",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="latest",
        help="use revision SPEC as restore source (default: %(default)s)",
    )
    p.add_argument(
        "target",
        metavar="TARGET",
        help='Copy backed up revision to TARGET. Use stdout if TARGET is "-"',
    )
    p.set_defaults(func="restore")

    p = subparsers.add_parser(
        "gc",
        help="Purge the backup store (i.e. chunked) from unused data",
    )
    p.add_argument(
        "--expire",
        action="store_true",
        help="Expire tags according to schedule",
    )
    p.set_defaults(func="gc")

    p = subparsers.add_parser(
        "rev-parse",
        help="Print full path or uuid of specified revisions",
    )
    p.add_argument(
        "--uuid",
        action="store_true",
        help="Print uuid instead of full path",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="latest",
        help="use revision SPEC to find (default: %(default)s)",
    )
    p.set_defaults(func="rev_parse")

    p = subparsers.add_parser(
        "log",
        help="Show backup status. Show inventory and summary information",
    )
    p.add_argument("--yaml", dest="yaml_", action="store_true")
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="all",
        help="use revision SPEC as filter (default: %(default)s)",
    )
    p.set_defaults(func="log_")

    # DISTRUST
    p = subparsers.add_parser(
        "distrust",
        help="Distrust specified revisions",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="local",
        help="use revision SPEC to distrust (default: %(default)s)",
    )
    p.set_defaults(func="distrust")

    p = subparsers.add_parser(
        "verify",
        help="Verify specified revisions",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="trust:distrusted&local",
        help="use revision SPEC to verify (default: %(default)s)",
    )
    p.set_defaults(func="verify")

    p = subparsers.add_parser(
        "forget",
        help="Forget specified revision",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        required=True,
        help="use revision SPEC to forget",
    )
    p.set_defaults(func="forget")

    p = subparsers.add_parser(
        "tags",
        help="Modify tags on revision",
    )
    p.add_argument(
        "--autoremove",
        action="store_true",
        help="Remove revision if no tags remain",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Do not validate tags"
    )
    p.add_argument(
        "--expect",
        metavar="<tags>",
        help="Do nothing if tags differ from the expected tags",
    )
    p.add_argument(
        "action",
        choices=["set", "add", "remove"],
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="all",
        help="modify tags for revision SPEC, modifies all if not given "
        "(default: %(default)s)",
    )
    p.add_argument(
        "tags",
        metavar="<tags>",
        help="comma separated list of tags",
    )
    p.set_defaults(func="tags")

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)

    task_id = generate_taskid()

    # Logging

    logging.init_logging(args.verbose, defaults={"taskid": task_id})
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    command = Command(args.C, args.config, task_id, log)
    func = args.func

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args["func"]
    del func_args["verbose"]
    del func_args["config"]
    del func_args["C"]

    try:
        log.debug("parsed", func=args.func, func_args=func_args)
        sys.exit(command(func, func_args))
    except Exception:
        log.exception("failed")
        sys.exit(1)
