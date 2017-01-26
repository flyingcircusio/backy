from .backends.chunked import ChunkedFileBackend
from backy.backup import Backup
from backy.nbd.server import Server
from backy.sources.file import File
import os


def file2chunk():
    """A very lightweight script to manually convert revisions to a chunk
    store."""
    # Rename the old data file so that we can use it as a new input
    # and re-create the old revision.
    backup = Backup('.')

    for old in backup.history:
        if old.backend_type != 'cowfile':
            continue
        print("Converting {}".format(old.uuid))
        os.rename(old.filename, old.filename + '.old')
        old.writable()
        new = ChunkedFileBackend(old)
        new.clone_parent = False
        source = File(dict(filename=old.filename + '.old', cow=False))(old)
        source.backup(new)
        old.readonly()
        # XXX os.unlink(old.filename)


def purge():
    backup = Backup('.')
    backup.scan()
    backend = backup.backend_factory(backup.history[0])
    store = backend.store
    for revision in backup.history:
        store.users.append(backup.backend_factory(revision).open())
    store.expunge()


def validate_revisions():
    backup = Backup('.')
    backup.scan()
    backend = backup.backend_factory(backup.history[0])
    print("Validating revisions")
    for revision in backup.history:
        print(revision.uuid)
        backend = backup.backend_factory(revision).open()
        for hash in backend._mapping.values():
            if os.path.exists(backend.store.chunk_path(hash)):
                continue
            if os.path.exists(
                    backend.store.chunk_path(hash, compressed=False)):
                continue
            print("Missing chunk {} in revision {}".format(
                  hash, revision.uuid))


def validate_chunks():
    backup = Backup('.')
    backup.scan()
    backend = backup.backend_factory(backup.history[0])
    print("Validating chunks")
    store = backend.store
    for progress in store.validate():
        print(progress)


def nbd():
    backup = Backup('.')
    backup.scan()
    server = Server(('127.0.0.1', 9000), backup)
    server.serve_forever()
