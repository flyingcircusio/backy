from typing import IO, Optional

from structlog.stdlib import BoundLogger

from backy.backends import BackyBackend
from backy.revision import Revision
from backy.utils import CHUNK_SIZE, cp_reflink


class COWFileBackend(BackyBackend):
    revision: Revision

    def __init__(self, revision: Revision, log: BoundLogger):
        assert revision.backend_type == "cowfile"
        self.revision = revision

    def open(self, mode: str = "rb", parent: Optional[Revision] = None) -> IO:
        if not self.revision.filename.exists():
            if not parent:
                self.revision.filename.open("wb").close()
            else:
                cp_reflink(parent.filename, self.revision.filename)
            self.revision.writable()
        return self.revision.filename.open(mode, buffering=CHUNK_SIZE)
