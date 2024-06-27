import argparse
import errno
import shutil
import sys
from pathlib import Path
from typing import Any

import structlog
from structlog.stdlib import BoundLogger

from backy.revision import Revision
from backy.source import Source
from backy.utils import generate_taskid

from .. import logging
from ..repository import Repository


class FileSource(Source):
    path: Path  # the source we are backing up
    type_ = "file"
    subcommand = "backy-file"

    def __init__(self, path: Path):
        self.path = path

    @classmethod
    def from_config(
        cls, config: dict[str, Any], log: BoundLogger
    ) -> "FileSource":
        return cls(path=Path(config["path"]))

    @staticmethod
    def to_config(path: Path) -> dict[str, Any]:
        return {"path": str(path)}

    def _path_for_revision(self, revision: Revision) -> Path:
        return self.repository.path / revision.uuid

    def backup(self, revision: Revision):
        backup = self._path_for_revision(revision)
        assert not backup.exists()
        shutil.copy(self.path, backup)

    def restore(self, revision: Revision, target: Path):
        shutil.copy(self._path_for_revision(revision), target)

    def gc(self):
        files = set(self.repository.path.glob("*"))
        expected_files = set(
            (self.repository.path / r.uuid)
            for r in self.repository.get_history()
        )
        for file in files - expected_files:
            file.unlink()

    def verify(self):
        for revision in self.repository.get_history():
            assert self._path_for_revision(revision).exists()


def main():
    parser = argparse.ArgumentParser(
        description="""Backup and restore for individual files.

This is mostly a dummy implementation to assist testing and development:
it is only able to back up from a single file and store versions of it
in a very simplistic fashion.
""",
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
        help="Remove unused data from the repository.",
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
        b = FileSource(args.backupdir, log)
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
