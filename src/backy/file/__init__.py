import shutil
import sys
import time
from argparse import _ActionsContainer
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from structlog.stdlib import BoundLogger

from backy.revision import Revision
from backy.source import RestoreArgs, Source

from ..repository import Repository


@dataclass(frozen=True)
class FileRestoreArgs(RestoreArgs):
    target: Path

    def to_cmdargs(self) -> Iterable[str]:
        return [str(self.target)]

    @classmethod
    def from_args(cls, **kw: Any) -> "FileRestoreArgs":
        return cls(kw["target"])

    @classmethod
    def setup_argparse(cls, restore_parser: _ActionsContainer) -> None:
        restore_parser.add_argument(
            "target",
            type=Path,
            metavar="TARGET",
            help="Copy backed up revision to TARGET",
        )


class FileSource(Source[FileRestoreArgs]):
    type_ = "file"
    restore_type = FileRestoreArgs

    filename: Path  # the source we are backing up

    def __init__(self, repository: Repository, filename: Path):
        super().__init__(repository)
        self.filename = filename

    @classmethod
    def from_config(
        cls, repository: Repository, config: dict[str, Any], log: BoundLogger
    ) -> "FileSource":
        assert config["type"] == "file"
        return cls(repository, Path(config["filename"]))

    # def to_config(self) -> dict[str, Any]:
    #     return {"type": self.type_, "path": str(self.path)}

    def _path_for_revision(self, revision: Revision) -> Path:
        return self.repository.path / revision.uuid

    def backup(self, revision: Revision):
        backup = self._path_for_revision(revision)
        assert not backup.exists()
        start = time.time()
        shutil.copy(self.filename, backup)
        revision.stats["duration"] = time.time() - start
        revision.write_info()
        revision.readonly()
        return True

    def restore(self, revision: Revision, args: FileRestoreArgs):
        shutil.copy(self._path_for_revision(revision), args.target)

    def gc(self):
        files = set(self.repository.path.glob("*.rev"))
        expected_files = set(
            (self.repository.path / r.uuid)
            for r in self.repository.get_history()
        )
        for file in files - expected_files:
            file.unlink()

    def verify(self, revision: Revision):
        assert self._path_for_revision(revision).exists()


def main():
    sys.exit(FileSource.main(*sys.argv))
