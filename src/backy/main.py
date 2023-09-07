# -*- encoding: utf-8 -*-

import argparse
import datetime
import errno
import sys
from pathlib import Path

import humanize
import structlog
import tzlocal
import yaml
from prettytable import PrettyTable
from structlog.stdlib import BoundLogger

import backy.backup
import backy.daemon
from backy.utils import format_datetime_local

from . import logging


def valid_date(s):
    if s is None:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


class Command(object):
    """Proxy between CLI calls and actual backup code."""

    path: str
    log: BoundLogger

    def __init__(self, path, log):
        self.path = path
        self.log = log

    def status(self, yaml_: bool):
        b = backy.backup.Backup(self.path, self.log)
        if yaml_:
            print(yaml.safe_dump([r.to_dict() for r in b.clean_history]))
            return
        total_bytes = 0

        tz = tzlocal.get_localzone()
        t = PrettyTable(
            [f"Date ({tz})", "ID", "Size", "Duration", "Tags", "Trust"]
        )
        t.align = "l"
        t.align["Size"] = "r"  # type: ignore
        t.align["Durat"] = "r"  # type: ignore

        for r in b.history:
            total_bytes += r.stats.get("bytes_written", 0)
            duration = r.stats.get("duration")
            if duration:
                duration = humanize.naturaldelta(duration)
            else:
                duration = "-"

            t.add_row(
                [
                    format_datetime_local(r.timestamp)[0],
                    r.uuid,
                    humanize.naturalsize(
                        r.stats.get("bytes_written", 0), binary=True
                    ),
                    duration,
                    ",".join(r.tags),
                    r.trust.value,
                ]
            )

        print(t)

        print(
            "{} revisions containing {} data (estimated)".format(
                len(b.history), humanize.naturalsize(total_bytes, binary=True)
            )
        )

    def backup(self, tags, force):
        b = backy.backup.Backup(self.path, self.log)
        b._clean()
        try:
            tags = set(t.strip() for t in tags.split(","))
            b.backup(tags, force)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            self.log.info("backup-already-running")
        finally:
            b._clean()

    def restore(self, revision, target):
        b = backy.backup.Backup(self.path, self.log)
        b.restore(revision, target)

    def forget(self, revision):
        b = backy.backup.Backup(self.path, self.log)
        b.forget_revision(revision)

    def scheduler(self, config):
        backy.daemon.main(config, self.log)

    def check(self, config):
        backy.daemon.check(config, self.log)

    def purge(self):
        b = backy.backup.Backup(self.path, self.log)
        b.purge()

    def upgrade(self):
        b = backy.backup.Backup(self.path, self.log)
        b.upgrade()

    def distrust(self, revision, from_, until):
        b = backy.backup.Backup(self.path, self.log)
        b.distrust(revision, from_, until)

    def verify(self, revision):
        b = backy.backup.Backup(self.path, self.log)
        b.verify(revision)


def setup_argparser():
    parser = argparse.ArgumentParser(
        description="Backup and restore for block devices.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="verbose output"
    )
    parser.add_argument(
        "-l",
        "--logfile",
        type=Path,
        default=argparse.SUPPRESS,
        help=(
            "file name to write log output in. "
            "(default: /var/log/backy.log for `scheduler` and `check`, "
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

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        "backup",
        help="""\
Perform a backup.
""",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Do not validate tags"
    )
    p.add_argument("tags", help="Tags to apply to the backup.")
    p.set_defaults(func="backup")

    # RESTORE
    p = subparsers.add_parser(
        "restore",
        help="""\
Restore (a given revision) to a given target.
""",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="latest",
        help="use revision SPEC as restore source",
    )
    p.add_argument(
        "target",
        metavar="TARGET",
        help="""\
Copy backed up revision to TARGET. Use stdout if TARGET is "-".
""",
    )
    p.set_defaults(func="restore")

    # BACKUP
    p = subparsers.add_parser(
        "purge",
        help="""\
Purge the backup store (i.e. chunked) from unused data.
""",
    )
    p.set_defaults(func="purge")

    # STATUS
    p = subparsers.add_parser(
        "status",
        help="""\
Show backup status. Show inventory and summary information.
""",
    )
    p.add_argument("--yaml", dest="yaml_", action="store_true")
    p.set_defaults(func="status")

    # upgrade
    p = subparsers.add_parser(
        "upgrade",
        help="""\
Upgrade this backup (incl. its data) to the newest supported version.
""",
    )
    p.set_defaults(func="upgrade")

    # SCHEDULER DAEMON
    p = subparsers.add_parser(
        "scheduler",
        help="""\
Run the scheduler.
""",
    )
    p.set_defaults(func="scheduler")
    p.add_argument("-c", "--config", default="/etc/backy.conf")

    # SCHEDULE CHECK
    p = subparsers.add_parser(
        "check",
        help="""\
Check whether all jobs adhere to their schedules' SLA.
""",
    )
    p.set_defaults(func="check")
    p.add_argument("-c", "--config", default="/etc/backy.conf")

    # DISTRUST
    p = subparsers.add_parser(
        "distrust",
        help="""\
Distrust one or all revisions.
""",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="",
        help="use revision SPEC to distrust, distrusting all if not given",
    )
    p.add_argument(
        "-f",
        "--from",
        metavar="DATE",
        type=valid_date,
        help="Mark revisions on or after this date as distrusted",
        dest="from_",
    )
    p.add_argument(
        "-u",
        "--until",
        metavar="DATE",
        type=valid_date,
        help="Mark revisions on or before this date as distrusted",
    )
    p.set_defaults(func="distrust")

    # VERIFY
    p = subparsers.add_parser(
        "verify",
        help="""\
Verify one or all revisions.
""",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        default="",
        help="use revision SPEC to verify, verifying all if not given",
    )
    p.set_defaults(func="verify")

    # FORGET
    p = subparsers.add_parser(
        "forget",
        help="""\
Forget revision.
""",
    )
    p.add_argument(
        "-r",
        "--revision",
        metavar="SPEC",
        required=True,
        help="use revision SPEC to forget",
    )
    p.set_defaults(func="forget")

    return parser


def main():
    parser = setup_argparser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)

    if not hasattr(args, "logfile"):
        args.logfile = None

    is_daemon = args.func == "scheduler" or args.func == "check"
    default_logfile = (
        Path("/var/log/backy.log")
        if is_daemon
        else args.backupdir / "backy.log"
    )

    # Logging
    logging.init_logging(
        args.verbose,
        args.logfile or default_logfile,
        default_job_name="-" if is_daemon else "",
    )
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    command = Command(args.backupdir, log)
    func = getattr(command, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args["func"]
    del func_args["verbose"]
    del func_args["backupdir"]
    del func_args["logfile"]

    try:
        log.debug("parsed", func=args.func, func_args=func_args)
        func(**func_args)
        log.debug("successful")
        sys.exit(0)
    except Exception:
        log.exception("failed")
        sys.exit(1)
