from typing import IO

import backy.revision
from backy.backends import BackyBackend
from backy.utils import CHUNK_SIZE, cp_reflink


class COWFileBackend(BackyBackend):
    revision: "backy.revision.Revision"

    def __init__(self, revision, log):
        self.revision = revision

    def open(self, mode: str = "rb") -> IO:
        if not self.revision.filename.exists():
            parent = self.revision.get_parent()
            if not parent:
                self.revision.filename.open("wb").close()
            else:
                cp_reflink(parent.filename, self.revision.filename)
            self.revision.writable()
        return self.revision.filename.open(mode, buffering=CHUNK_SIZE)
