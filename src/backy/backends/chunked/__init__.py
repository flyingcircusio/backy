import os.path
from .store import Store
from .file import File


class ChunkedFileBackend(object):

    def __init__(self, revision):
        self.backup = revision.backup
        self.revision = revision
        self.store = Store(path=self.revision.backup.path + '/chunks')
        self.clone_parent = True

    def open(self, mode='rb'):
        if 'w' in mode or '+' in mode and self.clone_parent:
            parent = self.revision.get_parent()
            if parent and not os.path.exists(self.revision.filename):
                with open(self.revision.filename, 'wb') as new, \
                        open(parent.filename, 'rb') as old:
                    # This is ok, this is just metadata, not the actual data.
                    new.write(old.read())
        overlay = False
        if mode == 'o':
            mode = 'rw'
            overlay = True
        return File(self.revision.filename, self.store, mode, overlay)

    def purge(self, backup):
        for revision in backup.history:
            self.store.users.append(backup.backend_factory(revision).open())
        self.store.purge()
