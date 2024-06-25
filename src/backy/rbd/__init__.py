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
        help="id to include in log messages (default: 4 random base32 chars)",
    )

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        "backup",
        help="Perform a backup",
    )
    p.add_argument("job", help="Which job to perform a backup for.")
    p.add_argument("revision", help="Revision to work on.")
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

    # XXX rename to "garbage collect"
    p = subparsers.add_parser(
        "purge",
        help="Purge the backup store (i.e. chunked) from unused data",
    )
    p.set_defaults(func="purge")

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

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_usage()
        sys.exit(0)

    logfile = args.backupdir / "backy.log"

    # Logging
    logging.init_logging(
        args.verbose,
        args.logfile or default_logfile,
        defaults={"taskid": args.taskid},
    )
    log = structlog.stdlib.get_logger(subsystem="command")
    log.debug("invoked", args=" ".join(sys.argv))

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args["func"]
    del func_args["verbose"]
    del func_args["backupdir"]
    del func_args["logfile"]
    del func_args["taskid"]

    try:
        log.debug("parsed", func=args.func, func_args=func_args)
        b = Backup(self.path, self.log)
        # XXX scheduler?
        b._clean()
        try:
            success = b.backup()
            ret = int(not success)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            self.log.warning("backup-currently-locked")
            ret = 1
        if isinstance(ret, int):
            log.debug("return-code", code=ret)
            sys.exit(ret)
        log.debug("successful")
        sys.exit(0)
    except Exception:
        log.exception("failed")
        sys.exit(1)
