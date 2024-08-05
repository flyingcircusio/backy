import argparse
import asyncio
import errno
import filecmp
import subprocess
from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Generic, Iterable, Optional, TypeVar, cast

import structlog
import yaml
from structlog.stdlib import BoundLogger

from backy import logging
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule
from backy.utils import SafeFile, generate_taskid

SOURCE_PLUGINS = entry_points(group="backy.sources")


def factory_by_type(type_) -> type["Source"]:
    return SOURCE_PLUGINS[type_].load()


RestoreArgsType = TypeVar("RestoreArgsType", bound="RestoreArgs")

SourceType = TypeVar("SourceType", bound="Source")


@dataclass(frozen=True)
class RestoreArgs(ABC):
    @abstractmethod
    def to_cmdargs(self) -> Iterable[str]:
        ...

    @classmethod
    @abstractmethod
    def setup_argparse(cls, restore_parser: ArgumentParser) -> None:
        ...

    @classmethod
    @abstractmethod
    def from_args(
        cls: type[RestoreArgsType], args: Namespace
    ) -> RestoreArgsType:
        ...


class Source(ABC, Generic[RestoreArgsType]):
    """A source provides specific implementations for making and restoring
    backups.

    There are three major aspects provided by a source implementation:

    1. Extracting data from another system (e.g. Ceph RBD or S3).

    2. Storing that data in the repository directory.

    3. Restoring data, typically providing different workflows:

    - full restore into the original system (e.g. into an RBD image)
    - full restore into another system (e.g. into a local image file)
    - partial restore (e.g. allowing interactive access to a loop mounted version of the image)

    Additionally a few house keeping tasks need to be implemented:

    - garbage collection, to remove data that isn't needed after revisions
      have expired

    - verification of stored data to protect against low level corruption


    Implementations can be split into two parts:

    - a light shim as a Python class that can interact with the
      rest of the backy code within Python

    - a subprocess that backy interacts with to trigger the actual work.

    """

    type_: str
    restore_type: type[RestoreArgsType]
    repository: "Repository"

    def __init__(self, repository: "Repository"):
        self.repository = repository

    @classmethod
    @abstractmethod
    def from_config(
        cls: type[SourceType],
        repository: "Repository",
        config: dict[str, Any],
        log: BoundLogger,
    ) -> SourceType:
        ...

    # @abstractmethod
    # def to_config(self) -> dict[str, Any]:
    # ...

    @abstractmethod
    def backup(self, revision: "Revision") -> bool:
        ...

    @abstractmethod
    def restore(self, revision: "Revision", args: RestoreArgsType):
        ...

    @abstractmethod
    def verify(self, revision: "Revision"):
        ...

    @abstractmethod
    def gc(self) -> None:
        ...

    @classmethod
    def create_argparse(cls) -> ArgumentParser:
        parser = argparse.ArgumentParser(
            description=f"The {cls.type_} plugin for backy. You should not call this directly. Use the backy command instead.",
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="verbose output"
        )
        # parser.add_argument(
        #     "-c",
        #     "--config",
        #     type=Path,
        #     default="/etc/backy.conf",
        #     help="(default: %(default)s)",
        # )
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
        # TODO: decide if the rev should be created
        p.add_argument("revision", help="Revision to create.")

        # RESTORE
        p = subparsers.add_parser(
            "restore",
            help="Restore (a given revision) to a given target",
        )
        p.add_argument("revision", help="Revision to restore.")
        cls.restore_type.setup_argparse(p)
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

        return parser

    @classmethod
    def main(cls, *str_args: str) -> int:
        parser = cls.create_argparse()

        args = parser.parse_args(str_args[1:])

        if not hasattr(args, "func"):
            parser.print_usage()
            return 0

        # Logging
        logging.init_logging(
            args.verbose,
            args.workdir / "backy.log",
            defaults={"taskid": args.taskid},
        )
        log = structlog.stdlib.get_logger(subsystem="command")
        log.debug("invoked", args=" ".join(str_args))

        try:
            source = CmdLineSource.load(args.workdir, log).create_source(cls)

            ret = 0
            match args.func:
                case "backup":
                    rev = source.repository.find_by_uuid(args.revision)
                    success = source.backup(rev)
                    ret = int(not success)
                case "restore":
                    rev = source.repository.find_by_uuid(args.revision)
                    source.restore(rev, cls.restore_type.from_args(args))
                case "gc":
                    source.gc()
                case "verify":
                    rev = source.repository.find_by_uuid(args.revision)
                    source.verify(rev)
                case _:
                    raise ValueError("invalid function: " + args.fun)
            log.debug("return-code", code=ret)
            return ret
        except Exception as e:
            if isinstance(e, IOError) and e.errno in [
                errno.EDEADLK,
                errno.EAGAIN,
            ]:
                log.warning("repo-currently-locked")
            else:
                log.exception("failed")
            return 1


class CmdLineSource:
    repository: "Repository"
    source_conf: dict[str, Any]
    log: BoundLogger

    @property
    def type_(self):
        return self.source_conf["type"]

    @property
    def subcommand(self) -> str:
        return "backy-" + self.type_

    @property
    def taskid(self):
        return self.log._context.get(
            "subtaskid", self.log._context.get("taskid", generate_taskid())
        )

    def __init__(
        self,
        repository: "Repository",
        source_conf: dict[str, Any],
        log: BoundLogger,
    ):
        self.repository = repository
        self.source_conf = source_conf
        self.log = log.bind(subsystem="cmdlinesource")

    @classmethod
    def from_config(
        cls, config: dict[str, Any], log: BoundLogger
    ) -> "CmdLineSource":
        schedule = Schedule()
        schedule.configure(config["schedule"])
        repo = Repository(Path(config["path"]), schedule, log)
        repo.connect()
        return cls(repo, config["source"], log)

    @classmethod
    def load(cls, path: Path, log: BoundLogger) -> "CmdLineSource":
        path = path / "config"
        try:
            with path.open(encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return cls.from_config(config, log)
        except IOError:
            log.error(
                "source-config-error",
                _fmt_msg="Could not read source config file. Is the path correct?",
                config_path=str(path),
            )
            raise

    def to_config(self) -> dict[str, Any]:
        return {
            "path": str(self.repository.path),
            "source": self.source_conf,
            "schedule": self.repository.schedule.config,
        }

    def store(self) -> None:
        """Writes config file for 'backy-<type>' subprocess."""

        # We do not want to create leading directories, only
        # the backup directory itself. If the base directory
        # does not exist then we likely don't have a correctly
        # configured environment.
        self.repository.path.mkdir(exist_ok=True)
        config = self.repository.path / "config"
        with SafeFile(config, encoding="utf-8") as f:
            f.open_new("wb")
            yaml.safe_dump(self.to_config(), f)
            if config.exists() and filecmp.cmp(config, f.name):
                raise ValueError("not changed")

    def create_source(
        self, sourcetype: Optional[type[SourceType]] = None
    ) -> SourceType:
        if sourcetype:
            sourcetype_ = sourcetype
        else:
            try:
                sourcetype_ = cast(
                    type[SourceType], factory_by_type(self.type_)
                )
            except KeyError:
                self.log.error(
                    "unknown-source-type",
                    _fmt_msg="Unknown source type '{type}'.",
                    type=self.type_,
                )
                raise

        return sourcetype_.from_config(
            self.repository, self.source_conf, self.log
        )

    def run(self, *args):
        return self.invoke(
            self.subcommand,
            "-t",
            self.taskid,
            "-C",
            str(self.repository.path),
            *args,
        )

    def invoke(self, *args):
        self.log.info("run", cmd=" ".join(args))
        proc = subprocess.run(args)
        self.log.debug("run-finished", return_code=proc.returncode)
        return proc.returncode

    def backup(self, revision: "Revision"):
        return self.run("backup", revision.uuid)

    def restore(self, revision: "Revision", args: RestoreArgsType):
        return self.run("restore", revision.uuid, *args.to_cmdargs())

    def verify(self, revision: "Revision"):
        return self.run("verify", revision.uuid)

    def gc(self):
        return self.run("gc")


class AsyncCmdLineSource(CmdLineSource):
    async def invoke(self, *args):
        self.log.info("run", cmd=" ".join(args))
        proc = await asyncio.create_subprocess_exec(
            *args,
            start_new_session=True,  # Avoid signal propagation like Ctrl-C.
            close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            return_code = await proc.wait()
            self.log.debug(
                "run-finished",
                return_code=return_code,
                subprocess_pid=proc.pid,
            )
            return return_code
        except asyncio.CancelledError:
            self.log.warning("run-cancelled", subprocess_pid=proc.pid)
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise
