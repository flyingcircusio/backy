from backy.utils import CHUNK_SIZE, cp_reflink
import os.path


class COWFileBackend(object):

    def __init__(self, revision):
        self.revision = revision

    def open(self, mode='rb'):
        if not os.path.exists(self.revision.filename):
            if not self.revision.parent:
                open(self.revision.filename, 'wb').close()
            else:
                parent = self.revision.backup.find(self.revision.parent)
                cp_reflink(parent.filename, self.revision.filename)
            self.revision.writable()
        return open(self.revision.filename, mode, buffering=CHUNK_SIZE)

    def purge(self, backup):
        # Nothing to do. This is a noop for cowfiles.
        pass

    def scrub(self, backup, type):
        # Nothing to do. This is a noop for cowfiles.
        pass
