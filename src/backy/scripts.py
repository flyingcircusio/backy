import argparse
from backy.backup import Backup
from backy.sources.file import File
import os


def file2chunk():
    """A very lightweight script to manually convert revisions to a chunk store."""
    parser = argparse.ArgumentParser(
        description='Convert a revision from CoW file to chunked.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('revision')
    args = parser.parse_args()

    # Rename the old data file so that we can use it as a new input
    # and re-create the old revision.
    backup = Backup('.')
    revision = backup.find(args.revision)
    os.rename(revision.filename, revision.filename + '.old')
    revision.writable()
    new = backup.backend_factory(revision)
    new.clone_parent = False
    source = File(dict(filename=revision.filename + '.old'))(revision)
    source.backup(new)
    revision.readonly()

def purge():
    backup = Backup('.')
    backup.scan()
    backend = backup.backend_factory(backup.history[0])
    store = backend.store
    for revision in backup.history:
        store.users.append(backup.backend_factory(revision).open())
    store.expunge()

def validate():
    backup = Backup('.')
    backup.scan()
    backend = backup.backend_factory(backup.history[0])
    store = backend.store
    print("Validating chunks")
    #store.validate()
    print("Validating revisions")
    for revision in backup.history:
        backend = backup.backend_factory(revision).open()
        for hash in backend._mapping.values():
            if not os.path.exists(backend.store.chunk_path(hash)):
                print("Missing chunk {} in revision {}".format(hash, revision.uuid))

