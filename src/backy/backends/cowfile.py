import os.path

import backy.revision
from backy.backends import BackyBackend
from backy.utils import CHUNK_SIZE, cp_reflink


class COWFileBackend(BackyBackend):
    revision: "backy.revision.Revision"

    def __init__(self, revision, log):
        self.revision = revision

    def open(self, mode="rb"):
        if not os.path.exists(self.revision.filename):
            parent = self.revision.get_parent()
            if not parent:
                open(self.revision.filename, "wb").close()
            else:
                cp_reflink(parent.filename, self.revision.filename)
            self.revision.writable()
        return open(self.revision.filename, mode, buffering=CHUNK_SIZE)
