# -*- encoding: utf-8 -*-

import argparse
import asyncio
import errno
import sys
from pathlib import Path
from typing import Literal, Optional

import humanize
import structlog
import tzlocal
import yaml
from aiohttp import ClientConnectionError
from rich import print as rprint
from rich.table import Column, Table
from structlog.stdlib import BoundLogger

import backy.daemon
from backy.utils import format_datetime_local, generate_taskid

from . import logging
from .backup import Backup, RestoreBackend
from .client import APIClient, CLIClient


class Command(object):
    """Proxy between CLI calls and actual backup code."""

    path: Path
    taskid: str
    log: BoundLogger

    def __init__(self, path: Path, taskid, log: BoundLogger):
        self.path = path
        self.taskid = taskid
        self.log = log

    def status(self, yaml_: bool, revision: str) -> None:
        revs = Backup(self.path, self.log).find_revisions(revision)
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
                f"[yellow]{pending_changes} pending change(s)[/] (Push changes with `backy push`)"
            )

    def backup(self, tags: str, force: bool) -> None:
        b = Backup(self.path, self.log)
        b._clean()
        try:
            tags_ = set(t.strip() for t in tags.split(","))
            b.backup(tags_, force)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            self.log.info("backup-already-running")
        finally:
            b._clean()

    def restore(
        self, revision: str, target: str, restore_backend: RestoreBackend
    ) -> None:
        b = Backup(self.path, self.log)
        b.restore(revision, target, restore_backend)

    def find(self, revision: str, uuid: bool) -> None:
        b = Backup(self.path, self.log)
        for r in b.find_revisions(revision):
            if uuid:
                print(r.uuid)
            else:
                print(r.filename)

    def forget(self, revision: str) -> None:
        b = Backup(self.path, self.log)
        b.forget(revision)
        b.warn_pending_changes()

    def scheduler(self, config: Path) -> None:
        backy.daemon.main(config, self.log)

    def purge(self) -> None:
        b = Backup(self.path, self.log)
        b.purge()

    def upgrade(self) -> None:
        b = Backup(self.path, self.log)
        b.upgrade()

    def distrust(self, revision: str) -> None:
        b = Backup(self.path, self.log)
        b.distrust(revision)

    def verify(self, revision: str) -> None:
        b = Backup(self.path, self.log)
        b.verify(revision)

    def client(
        self,
        config: Path,
        peer: str,
        url: str,
        token: str,
        apifunc: str,
        **kwargs,
    ) -> None:
        async def run():
            if url and token:
                api = APIClient("<server>", url, token, self.taskid, self.log)
            else:
                d = backy.daemon.BackyDaemon(config, self.log)
                d._read_config()
                if peer:
                    api = APIClient.from_conf(
                        peer, d.peers[peer], self.taskid, self.log
                    )
                else:
                    api = APIClient.from_conf(
                        "<server>", d.api_cli_default, self.taskid, self.log
                    )
            async with CLIClient(api, self.log) as c:
                try:
                    await getattr(c, apifunc)(**kwargs)
                except ClientConnectionError as e:
                    c.log.error("connection-error", exc_style="banner")
                    c.log.debug("connection-error", exc_info=True)
                    sys.exit(1)

        asyncio.run(run())

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
        b = backy.backup.Backup(self.path, self.log)
        success = b.tags(
            action,
            revision,
            tags_,
            expect=expect_,
            autoremove=autoremove,
            force=force,
        )
        b.warn_pending_changes()
        return int(not success)

    def expire(self) -> None:
        b = backy.backup.Backup(self.path, self.log)
        b.expire()
        b.warn_pending_changes()

    def push(self, config: Path):
        d = backy.daemon.BackyDaemon(config, self.log)
        d._read_config()
        b = backy.backup.Backup(self.path, self.log)
        asyncio.run(b.push_metadata(d.peers, self.taskid))

    def pull(self, config: Path):
        d = backy.daemon.BackyDaemon(config, self.log)
        d._read_config()
        b = backy.backup.Backup(self.path, self.log)
        asyncio.run(b.pull_metadata(d.peers, self.taskid))


def setup_argparser():
    parser = argparse.ArgumentParser(
        description="Backup and restore for block devices.",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="verbose output"
    )
    parser.add_argument(
        "-l",
        "--logfile",
        type=Path,
        help=(
            "file name to write log output in. "
            "(default: /var/log/backy.log for `scheduler`, ignored for `client`, "
            "$backupdir/backy.log otherwise)"
        ),
    )
    parser.add_argument(
        "-b",
        "--backupdir",
        default=".",
        type=Path,
        help=(
            "directory where backups and logs are written to "
            "(default: %(default)s)"
        ),
    )
    parser.add_argument(
        "-t",
        "--taskid",
        default=generate_taskid(),
        help="id to include in log messages (default: 4 random base32 chars)",
    )

    subparsers = parser.add_subparsers()

    # CLIENT
    client = subparsers.add_parser(
        "client",
        help="Query the api",
    )
    g = client.add_argument_group()
    g.add_argument(
        "-c",
        "--config",
        type=Path,
        default="/etc/backy.conf",
        help="(default: %(default)s)",
    )
    g.add_argument("-p", "--peer", help="(default: read from config file)")
    g = client.add_argument_group()
    g.add_argument("--url")
    g.add_argument("--token")
    client.set_defaults(func="client")
    client_parser = client.add_subparsers()

    # CLIENT jobs
    p = client_parser.add_parser("jobs", help="List status of all known jobs")
    p.add_argument(
        "filter_re",
        default="",
        metavar="[filter]",
        nargs="?",
        help="Optional job filter regex",
    )
    p.set_defaults(apifunc="jobs")

    # CLIENT status
    p = client_parser.add_parser("status", help="Show job status overview")
    p.set_defaults(apifunc="status")

    # CLIENT run
    p = client_parser.add_parser(
        "run", help="Trigger immediate run for one job"
    )
    p.add_argument("job", metavar="<job>", help="Name of the job to run")
    p.set_defaults(apifunc="run")

    # CLIENT runall
    p = client_parser.add_parser(
        "runall", help="Trigger immediate run for all jobs"
    )
    p.set_defaults(apifunc="runall")

    # CLIENT reload
    p = client_parser.add_parser("reload", help="Reload the configuration")
    p.set_defaults(apifunc="reload")

    # CLIENT check
    p = client_parser.add_parser(
        "check",
        help="Check whether all jobs adhere to their schedules' SLA",
    )
    p.set_defaults(apifunc="check")

    # BACKUP
    p = subparsers.add_parser(
        "backup",
        help="Perform a backup",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Do not validate tags"
    )
    p.add_argument("tags", help="Tags to apply to the backup")
    p.set_defaults(func="backup")

    # RESTORE
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

    # BACKUP
    p = subparsers.add_parser(
        "purge",
        help="Purge the backup store (i.e. chunked) from unused data",
    )
    p.set_defaults(func="purge")

    # FIND
    p = subparsers.add_parser(
        "find",
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
    p.set_defaults(func="find")

    # STATUS
    p = subparsers.add_parser(
        "status",
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
    p.set_defaults(func="status")

    # upgrade
    p = subparsers.add_parser(
        "upgrade",
        help="Upgrade this backup (incl. its data) to the newest supported version",
    )
    p.set_defaults(func="upgrade")

    # SCHEDULER DAEMON
    p = subparsers.add_parser(
        "scheduler",
        help="Run the scheduler",
    )
    p.set_defaults(func="scheduler")
    p.add_argument(
        "-c",
        "--config",
        type=Path,
        default="/etc/backy.conf",
        help="(default: %(default)s)",
    )

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

    # FORGET
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
        "action",
        choices=["set", "add", "remove"],
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="all",
        help="modify tags for revision SPEC, modifies all if not given (default: %(default)s)",
    )
    p.add_argument(
        "tags",
        metavar="<tags>",
        help="comma separated list of tags",
    )
    p.set_defaults(func="tags")

    # EXPIRE
    p = subparsers.add_parser(
        "expire",
        help="Expire tags according to schedule",
    )
    p.set_defaults(func="expire")

    # PUSH
    p = subparsers.add_parser(
        "push",
        help="Push pending changes to remote servers",
    )
    p.add_argument(
        "-c",
        "--config",
        type=Path,
        default="/etc/backy.conf",
        help="(default: %(default)s)",
    )
    p.set_defaults(func="push")

    # PULL
    p = subparsers.add_parser(
        "pull",
        help="Push pending changes to remote servers",
    )
    p.add_argument(
        "-c",
        "--config",
        type=Path,
        default="/etc/backy.conf",
        help="(default: %(default)s)",
    )
    p.set_defaults(func="pull")

    return parser, client


def main():
    parser, client_parser = setup_argparser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)
    if args.func == "client" and not hasattr(args, "apifunc"):
        client_parser.print_usage()
        sys.exit(0)

    default_logfile: Optional[Path]
    match args.func:
        case "scheduler":
            default_logfile = Path("/var/log/backy.log")
        case "client":
            default_logfile = None
        case _:
            default_logfile = args.backupdir / "backy.log"

    match (args.func, vars(args).get("apifunc")):
        case ("scheduler", _):
            default_job_name = "-"
        case ("client", "check"):
            default_job_name = "-"
        case _:
            default_job_name = ""

    # Logging
    logging.init_logging(
        args.verbose,
        args.logfile or default_logfile,
        defaults={"job_name": default_job_name, "taskid": args.taskid},
    )
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    command = Command(args.backupdir, args.taskid, log)
    func = getattr(command, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args["func"]
    del func_args["verbose"]
    del func_args["backupdir"]
    del func_args["logfile"]
    del func_args["taskid"]

    try:
        log.debug("parsed", func=args.func, func_args=func_args)
        func(**func_args)
        log.debug("successful")
        sys.exit(0)
    except Exception:
        log.exception("failed")
        sys.exit(1)
