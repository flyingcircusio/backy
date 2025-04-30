import argparse
import asyncio
import inspect
import re
import sys
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import humanize
import structlog
import tzlocal
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
from backy.repository import Repository
from backy.revision import Revision, filter_manual_tags
from backy.schedule import Schedule
from backy.source import SOURCE_PLUGINS, CmdLineSource
from backy.utils import BackyJSONEncoder, format_datetime_local, generate_taskid

# single repo commands


# (init)

# rev-parse (job?, rev)               Print full path or uuid of specified revisions

# log [--filter] (status) (rev)             Show backup status. Show inventory and summary information

# backup [--bg] (job)           Perform a backup
# restore                      Restore (a given revision) to a given target

# distrust (job, rev)            Distrust specified revisions
# verify (job, rev)             Verify specified revisions
# rm  (job, rev)                Forget specified revision
# tag  (job, rev)               Modify tags on revision

# gc [--expire] [--remote|--local] (job)      (Expire revisions) and collect garbage from the repository.

# pull? (job)              update metadata from all known remotes that host backups
#                     for the same backup source


# reports list/show/delete


# # multi-repo / daemon-based commands

# check (job)
# show-jobs (job def: all)          List status of all known jobs (integrated with log?)
# show-daemon         Daemon status
# reload

# maybe add a common --repo/--job <regex> flag?


class Command(object):
    """Proxy between CLI calls and actual backup code."""

    path: Path
    config: Path
    dry_run: bool
    jobs: Optional[str]
    log: BoundLogger

    def __init__(
        self,
        path: Path,
        config: Path,
        dry_run: bool,
        jobs: Optional[str],
        log: BoundLogger,
    ):
        self.path = path.resolve()
        self.config = config
        self.dry_run = dry_run
        self.jobs = jobs
        self.log = log.bind(subsystem="command")

    async def __call__(self, cmdname: str, kwargs: dict[str, Any]):
        self.log.debug("call", func=cmdname, func_args=kwargs)
        try:
            func = getattr(self, cmdname)
            params = inspect.signature(func).parameters
            ret = 0
            if "repo" in params and params["repo"].annotation == Repository:
                for repo in await self.get_repos():
                    r = func(repo=repo, **kwargs)
                    if asyncio.iscoroutine(r):
                        r = await r
                    if not isinstance(r, int):
                        r = 0
                    ret = max(ret, r)
            elif (
                "repos" in params
                and params["repos"].annotation == List[Repository]
            ):
                ret = func(repos=await self.get_repos(), **kwargs)
                if asyncio.iscoroutine(ret):
                    ret = await ret
                if not isinstance(ret, int):
                    ret = 0
            else:
                assert (
                    self.jobs is None
                ), "This subcommand does not support --jobs/-a"
                ret = func(**kwargs)
                if asyncio.iscoroutine(ret):
                    ret = await ret
                if not isinstance(ret, int):
                    ret = 0
            self.log.debug("return-code", code=ret)
            return ret
        except Exception:
            self.log.exception("failed")
            return 1

    @cached_property
    def source(self) -> CmdLineSource:
        return CmdLineSource.load(self.path, self.log)

    @cached_property
    def api(self):
        d = BackyDaemon(self.config, self.log)
        d._read_config()
        taskid = self.log._context.get("taskid", generate_taskid())
        return Client.from_conf("<server>", d.api_cli_default, taskid, self.log)

    async def get_repos(self) -> List[Repository]:
        if self.jobs is None:
            return [self.source.repository]
        else:
            jobs = await self.api.get_jobs()
            assert len(jobs) > 0, "daemon has no configured job"
            reg = re.compile(self.jobs)
            res = [
                Repository(
                    Path(job["path"]),
                    Schedule.from_dict(job["schedule"]),
                    self.log,
                )
                for job in jobs
                if reg.search(job["name"])
            ]
            assert len(res) > 0, "--jobs filter did not match"
            for r in res:
                r.connect()
            return res

    #
    # def init(self, type):
    #     sourcefactory = backy.source.factory_by_type(type)
    #     source = sourcefactory(*sourcefactory.argparse())
    #     # TODO: check if repo already exists
    #     repo = Repository(self.path / "config", source, Schedule(), self.log)
    #     repo.connect()
    #     repo.store()

    def rev_parse(self, repo: Repository, revision: str, uuid: bool) -> None:
        for rev in repo.find_revisions(revision):
            if uuid:
                print(rev.uuid)
            else:
                print(rev.info_filename)

    def log_(self, repo: Repository, json_: bool, revision: str) -> None:
        revs = repo.find_revisions(revision)
        if json_:
            print(BackyJSONEncoder().encode([r.to_dict() for r in revs]))
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
                (
                    f"[underline italic]{r.server}[/]"
                    if r.pending_changes
                    else r.server
                ),
            )

        rprint(t)

        print(
            "{} revisions containing {} data (estimated)".format(
                len(revs), humanize.naturalsize(total_bytes, binary=True)
            )
        )

    async def backup(
        self, repos: List[Repository], bg: bool, tags: str, force: bool
    ) -> int:
        if len(repos) > 1:
            bg = True

        if bg:
            for repo in repos:
                log = self.log.bind(job_name=repo.name)
                try:
                    # TODO support tags
                    await self.api.run_job(repo.name)
                    log.info("triggered-run")
                except ClientResponseError as e:
                    if e.status == HTTPNotFound.status_code:
                        log.error("unknown-job")
                        return 1
                    raise
            return 0
        else:
            repo = repos[0]
            assert (
                self.source.repository.path == repo.path
            ), "only the current job is supported without --bg"
            repo._clean()
            tags_ = set(t.strip() for t in tags.split(","))
            if not force:
                repo.validate_tags(tags_)
            r = Revision.create(repo, tags_, self.log)
            r.materialize()
            try:
                return self.source.backup(r)
            finally:
                repo._clean()

    def restore(
        self,
        revision: str,
        **restore_args: Any,
    ) -> int:
        r = self.source.repository.find(revision)
        return self.source.restore(
            r, self.source.restore_type.from_args(**restore_args)
        )

    def distrust(self, repo: Repository, revision: str) -> None:
        repo.distrust(repo.find_revisions(revision))

    def verify(self, revision: str) -> int:
        # TODO support multiple repos
        ret = 0
        for r in self.source.repository.find_revisions(revision):
            ret = max(ret, self.source.verify(r))
        return ret

    def rm(self, repo: Repository, revision: str) -> None:
        repo.rm(repo.find_revisions(revision))

    def tags(
        self,
        repo: Repository,
        tag_action: Literal["set", "add", "remove"],
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
        success = repo.tags(
            tag_action,
            revision,
            tags_,
            expect=expect_,
            autoremove=autoremove,
            force=force,
        )
        return int(not success)

    def gc(self, repo: Repository, expire: bool, local: bool) -> None:
        if expire and not local:
            assert False  # request pull from daemon
            # XXX needs to update from remote API peers first (pull)
        assert self.source.repository.path == repo.path
        if expire:
            repo.expire()
        if expire and not local:
            assert False  # request push from daemon
        self.source.gc()

    def reports_list(self, repo: Repository):
        for id in repo.report_ids:
            print(id)

    def reports_show(self, repo: Repository, reports: Optional[str]):
        if reports is None:
            ids = repo.report_ids
        else:
            ids = reports.split(",")
        for id in ids:
            path = repo.report_path.joinpath(id).with_suffix(".report")
            print(id)
            print(path.read_text(encoding="utf-8"))

    def reports_delete(self, repo: Repository, reports: Optional[str]):
        log = self.log.bind(job_name=repo.name)
        if reports is None:
            ids = repo.report_ids
        else:
            ids = reports.split(",")
        for id in ids:
            path = repo.report_path.joinpath(id).with_suffix("report")
            path.unlink()
            log.info("report-deleted", id=id)

    def check(self, repo: Repository):
        log = self.log.bind(job_name=repo.name)
        exitcode = 0

        manual_tags = set()
        for rev in repo.history:
            manual_tags |= filter_manual_tags(rev.tags)
        if manual_tags:
            log.info("check-manual-tags", manual_tags=", ".join(manual_tags))

        unsynced_revs = {r for r in repo.history if r.pending_changes}
        if unsynced_revs:
            log.info("check-unsynced-revs", unsynced_revs=len(unsynced_revs))

        if not repo.sla:
            log.critical(
                "check-sla-violation",
                last_time=str(
                    repo.clean_history[-1].timestamp
                    if repo.clean_history
                    else None
                ),
                sla_overdue=repo.sla_overdue,
            )
            exitcode = max(exitcode, 2)

        if repo.report_ids:
            log.warning("check-reports", reports=len(repo.report_ids))
            exitcode = max(exitcode, 1)

        return exitcode

    async def show_jobs(self, repos: List[Repository]):
        """List status of all known jobs. Optionally filter by regex."""
        repo_names = [r.name for r in repos]

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

        jobs = await self.api.fetch_status(self.jobs)
        jobs.sort(key=lambda j: j["job"])
        for job in jobs:
            if job["job"] not in repo_names:
                continue
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
        if self.jobs:
            backups = list(filter(re.compile(self.jobs).search, backups))
        for b in backups:
            t.add_row(b, "-", "-", "Dead", "-", "", "-", "-", "")

        rprint(t)
        print("{} jobs shown".format(len(jobs) + len(backups)))

    async def show_daemon(self):
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

    async def reload_daemon(self):
        """Reload the configuration."""
        await self.api.reload_daemon()


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
        dest="workdir",
        default=".",
        type=Path,
        help=(
            "Run as if backy was started in <path> instead of the current "
            "working directory."
        ),
    )

    parser.add_argument(
        "-n", "--dry-run", action="store_true", help="Do not modify state."
    )
    job_filter = parser.add_mutually_exclusive_group()
    job_filter.add_argument(
        "--jobs",
        dest="jobs",
        metavar="<job filter>",
        help="Optional job filter regex. Defaults to current workdir",
    )

    job_filter.add_argument(
        "-a",
        "--all",
        action="store_const",
        const=".*",
        dest="jobs",
        help="Shortcut to select all jobs",
    )

    subparsers = parser.add_subparsers()

    # TODO
    # INIT
    p = subparsers.add_parser("init", help="Create an empty backy repository.")
    p.add_argument(
        "type",
        choices=backy.source.SOURCE_PLUGINS.names,
        help="Type of the source.",
    )
    p.set_defaults(func="init")

    # REV-PARSE
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
        default="all",
        help="use revision SPEC to find (default: %(default)s)",
    )
    p.set_defaults(func="rev_parse")

    # LOG
    p = subparsers.add_parser(
        "log",
        help="Show backup status. Show inventory and summary information",
    )
    p.add_argument("--json", dest="json_", action="store_true")
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="all",
        help="use revision SPEC as filter (default: %(default)s)",
    )
    p.set_defaults(func="log_")

    # BACKUP
    p = subparsers.add_parser(
        "backup",
        help="Perform a backup",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Do not validate tags"
    )
    p.add_argument(
        "--bg",
        action="store_true",
        help="Let the daemon run the backup job. Implied if if more than one job is selected.",
    )
    p.add_argument("tags", help="Tags to apply to the backup")
    p.set_defaults(func="backup")

    # RESTORE
    p = subparsers.add_parser(
        "restore",
        help="Restore (a given revision) to a given target. The arguments vary for the different repo types.",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="latest",
        help="use revision SPEC as restore source (default: %(default)s)",
    )
    restore_subparsers = p.add_subparsers()
    for source_type in SOURCE_PLUGINS:
        source = source_type.load()
        source.restore_type.setup_argparse(
            restore_subparsers.add_parser(source.type_)
        )
    p.set_defaults(func="restore")

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

    # VERIFY
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

    # RM
    p = subparsers.add_parser(
        "rm",
        help="Remove specified revision",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        required=True,
        help="use revision SPEC to remove",
    )
    p.set_defaults(func="rm")

    # TAGS
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
        "tag_action",
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

    # GC
    p = subparsers.add_parser(
        "gc",
        help="Purge the backup store (i.e. chunked) from unused data",
    )
    p.add_argument(
        "--expire",
        action="store_true",
        help="Expire tags according to schedule",
    )
    p.add_argument(
        "--local",
        action="store_true",
        help="Do not expire on remote servers",
    )
    p.set_defaults(func="gc")

    # REPORTS-LIST
    p = subparsers.add_parser("reports-list", help="List problem reports")
    p.set_defaults(func="reports_list")

    # REPORTS-SHOW
    p = subparsers.add_parser("reports-show", help="Show problem report")
    p.add_argument(
        "reports",
        nargs="?",
        metavar="<reports>",
        help="comma separated list of report uuids",
    )

    p.set_defaults(func="reports_show")

    # REPORTS-DELETE
    p = subparsers.add_parser("reports-delete", help="Delete problem report")
    report_sel = p.add_mutually_exclusive_group(required=True)
    report_sel.add_argument(
        "reports",
        nargs="?",
        metavar="<reports>",
        help="comma separated list of report uuids",
    )
    report_sel.add_argument(
        "--all-reports",
        action="store_const",
        const=None,
        dest="reports",
        help="Select all reports",
    )
    p.set_defaults(func="reports_delete")

    # CHECK
    p = subparsers.add_parser(
        "check",
        help="Check whether the selected jobs adhere to their schedules' SLA",
    )
    p.set_defaults(func="check")

    # TODO: job filter default
    # SHOW JOBS
    p = subparsers.add_parser("show-jobs", help="List status of all known jobs")
    p.set_defaults(func="show_jobs")

    # SHOW DAEMON
    p = subparsers.add_parser("show-daemon", help="Show job status overview")
    p.set_defaults(func="show_daemon")

    # RELOAD DAEMON
    p = subparsers.add_parser("reload-daemon", help="Reload daemon config")
    p.set_defaults(func="reload_daemon")

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)

    # Logging

    logging.init_logging(args.verbose, defaults={"taskid": generate_taskid()})
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    command = Command(args.workdir, args.config, args.dry_run, args.jobs, log)
    func = args.func

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args["func"]
    del func_args["verbose"]
    del func_args["workdir"]
    del func_args["config"]
    del func_args["dry_run"]
    del func_args["jobs"]

    sys.exit(asyncio.run(command(func, func_args)))
