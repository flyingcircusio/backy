from pathlib import Path
from typing import IO, Optional, Set

from structlog.stdlib import BoundLogger

from backy.revision import Revision, Trust
from backy.utils import END, report_status

from ...backup import Backup
from .. import BackyBackend
from .bucketsnapshot import BucketSnapshot
from .store import Store


class S3Backend(BackyBackend):
    # multiple Backends may share the same store
    STORES: dict[Path, Store] = dict()

    backup: Backup
    revision: Revision
    store: Store
    log: BoundLogger

    def __init__(self, revision: Revision, log: BoundLogger):
        assert revision.backend_type == "s3"
        self.backup = revision.backup
        self.revision = revision
        path = self.backup.path / "objects"
        if path not in self.STORES:
            self.STORES[path] = Store(path, log)
        self.store = self.STORES[path]
        self.log = log.bind(subsystem="s3")

    def open(self, mode: str = "rb", parent: Optional["Revision"] = None) -> IO:
        raise NotImplementedError("s3 only supports open_multi")

    def open_multi(
        self,
        mode: str = "rb",
        parent: Optional[Revision] = None,
    ):
        return BucketSnapshot(self.store, mode, self.revision, parent, self.log)

    def purge(self) -> None:
        self.store.purge(
            {r.uuid for r in self.backup.history if r.backend_type == "s3"}
        )

    @report_status
    def verify(self):
        self.store.verify(self.revision)
