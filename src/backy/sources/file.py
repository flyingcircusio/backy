from typing import Optional

from structlog.stdlib import BoundLogger

import backy.backends
from backy.quarantine import QuarantineReport
from backy.revision import Revision, Trust
from backy.sources import BackySource, BackySourceContext, BackySourceFactory
from backy.utils import copy, copy_overwrite, files_are_equal


class File(BackySource, BackySourceFactory, BackySourceContext):
    filename: str
    cow: bool
    revision: Revision
    log: BoundLogger

    def __init__(self, config: dict, log: BoundLogger):
        self.filename = config["filename"]
        self.cow = config.get("cow", True)
        self.log = log.bind(filename=self.filename, subsystem="file")

    def __call__(self, revision: Revision):
        self.revision = revision
        self.log = self.log.bind(revision_uuid=revision.uuid)
        return self

    def __enter__(self):
        return self

    def ready(self) -> bool:
        """Check whether the source can be backed up.

        For files this means the file exists and is readable.

        """
        try:
            with open(self.filename, "rb"):
                pass
        except Exception:
            return False
        return True

    def backup(self, target: "backy.backends.BackyBackend") -> None:
        self.log.debug("backup")
        s = open(self.filename, "rb")
        parent = self.revision.get_parent()
        with s as source, target.open("r+b", parent) as target_:
            if self.cow and parent:
                self.log.info("backup-sparse")
                bytes = copy_overwrite(source, target_)
            else:
                self.log.info("backup-full")
                bytes = copy(source, target_)

        self.revision.stats["bytes_written"] = bytes

    def verify(self, target: "backy.backends.BackyBackend") -> bool:
        self.log.info("verify")
        s = open(self.filename, "rb")
        with s as source, target.open("rb") as target_:
            return files_are_equal(
                source,
                target_,
                report=lambda s, t, o: self.revision.backup.quarantine.add_report(
                    QuarantineReport(s, t, o)
                ),
            )
