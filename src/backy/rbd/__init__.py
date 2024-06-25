import argparse
import errno
import sys
from pathlib import Path

import structlog

from backy.utils import generate_taskid

from .. import logging
from .backup import RbdBackup, RestoreBackend


def main():
    parser = argparse.ArgumentParser(
        description="Backup and restore for block devices.",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="verbose output"
    )
    parser.add_argument(
        "-t",
        "--taskid",
        default=generate_taskid(),
        help="ID to include in log messages (default: 4 random base32 chars)",
    )
    parser.add_argument("-j", "--job", help="Job to work on.")

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        "backup",
        help="Perform a backup",
    )
    p.set_defaults(func="backup")
    parser.add_argument("-r", "--revision", help="Revision to work on.")

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
    parser.add_argument("-r", "--revision", help="Revision to work on.")
    p.add_argument(
        "target",
        metavar="TARGET",
        help='Copy backed up revision to TARGET. Use stdout if TARGET is "-"',
    )
    p.set_defaults(func="restore")

    # GC
    p = subparsers.add_parser(
        "gc",
        help="Purge the backup store from unused data",
    )
    p.set_defaults(func="gc")

    # VERIFY
    p = subparsers.add_parser(
        "verify",
        help="Verify specified revision",
    )
    parser.add_argument("-r", "--revision", help="Revision to work on.")
    p.set_defaults(func="verify")

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)

    backupdir = Path("/srv/backy/" + args.job)  # TODO

    # Logging
    logging.init_logging(
        args.verbose,
        backupdir / "backy.log",
        defaults={"taskid": args.taskid},
    )
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    try:
        b = RbdBackup(backupdir, log)
        # XXX scheduler?
        b._clean()
        ret = 0
        match args.fun:
            case "backup":
                success = b.backup(args.revision)
                ret = int(not success)
            case "restore":
                b.restore(args.revisions, args.target, args.backend)
            case "gc":
                b.gc()
            case "verify":
                b.verify(args.revision)
            case _:
                raise ValueError("invalid function: " + args.fun)
        log.debug("return-code", code=ret)
        sys.exit(ret)
    except Exception as e:
        if isinstance(e, IOError) and e.errno in [errno.EDEADLK, errno.EAGAIN]:
            log.warning("backup-currently-locked")
        else:
            log.exception("failed")
        sys.exit(1)
