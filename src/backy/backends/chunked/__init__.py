import os.path
from .store import Store
from .file import File


class ChunkedFileBackend(object):

    # Normally a new revision will be made by copying the last revision's file.
    # We need to be able to not do this in case of converting from a different
    # format.
    clone_parent = True

    def __init__(self, revision):
        self.backup = revision.backup
        self.revision = revision
        self.store = Store(path=self.revision.backup.path + '/chunks')

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
            try:
                self.store.users.append(
                    backup.backend_factory(revision).open())
            except ValueError:
                # Invalid format, like purging non-chunked with chunked backend
                pass
        self.store.purge()

    def scrub(self, backup, type):
        if type == 'light':
            return self.scrub_light(backup)
        elif type == 'deep':
            return self.scrub_deep(backup)
        else:
            raise RuntimeError('Invalid scrubbing type {}'.format(type))

    def scrub_light(self, backup):
        errors = 0
        print("Validating revisions")
        for revision in backup.history:
            print(revision.uuid)
            backend = backup.backend_factory(revision).open()
            for hash in backend._mapping.values():
                if os.path.exists(backend.store.chunk_path(hash)):
                    continue
                print("Missing chunk {} in revision {}".format(
                      hash, revision.uuid))
                errors += 1
        return errors

    def scrub_deep(self, backup):
        errors = self.scrub_light(backup)
        print("Validating chunks")
        errors += self.store.validate_chunks()
        return errors
