from structlog.stdlib import BoundLogger

from backy.revision import Revision
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

    def ready(self):
        """Check whether the source can be backed up.

        For files this means the file exists and is readable.

        """
        try:
            with open(self.filename, "rb"):
                pass
        except Exception:
            return False
        return True

    def backup(self, target):
        self.log.debug("backup")
        s = open(self.filename, "rb")
        t = target.open("r+b")
        with s as source, t as target:
            if self.cow:
                self.log.info("backup-sparse")
                bytes = copy_overwrite(source, target)
            else:
                self.log.info("backup-full")
                bytes = copy(source, target)

        self.revision.stats["bytes_written"] = bytes

    def verify(self, target):
        try:
            self.log.info("verify")
            s = open(self.filename, "rb")
            t = target.open("rb")
            with s as source, t as target:
                return files_are_equal(source, target)
        except Exception:
            self.log.exception("verify-failed")
            return False
