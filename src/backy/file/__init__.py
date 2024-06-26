import argparse
import errno
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from structlog.stdlib import BoundLogger

from backy.utils import generate_taskid

from .. import logging

if TYPE_CHECKING:
    from backy.repository import Repository


class FileSource:
    type_ = "file"

    @classmethod
    def init(cls, repository: "Repository", log: BoundLogger):
        return {"type": cls.type_}


def main():
    parser = argparse.ArgumentParser(
        description="Backup and restore for block devices.",
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
    parser.add_argument(
        "-t",
        "--taskid",
        default=generate_taskid(),
        help="ID to include in log messages (default: 4 random base32 chars)",
    )

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        "backup",
        help="Perform a backup",
    )
    p.set_defaults(func="backup")
    p.add_argument("revision", help="Revision to create.")

    # RESTORE
    p = subparsers.add_parser(
        "restore",
        help="Restore (a given revision) to a given target",
    )
    p.add_argument("revision", help="Revision to restore.")
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
    p.add_argument("revision", help="Revision to work on.")
    p.set_defaults(func="verify")

    args = parser.parse_args()

    os.chdir(args.C)

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)

    # Logging
    logging.init_logging(
        args.verbose,
        args.backupdir / "backy.log",
        defaults={"taskid": args.taskid},
    )
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    try:
        b = RbdRepository(args.backupdir, log)
        # XXX scheduler?
        b._clean()
        ret = 0
        match args.func:
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
