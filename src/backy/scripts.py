import argparse
from backy.backup import Backup
from backy.sources.file import File
import os


def file2chunk():
    """A very lightweight script to manually convert revisions to a chunk
    store."""
    parser = argparse.ArgumentParser(
        description='Convert a revision from CoW file to chunked.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('revision')
    args = parser.parse_args()

    # Rename the old data file so that we can use it as a new input
    # and re-create the old revision.
    backup = Backup('.')
    old = backup.find(args.old)
    os.rename(old.filename, old.filename + '.old')
    old.writable()
    new = backup.backend_factory(old)
    new.clone_parent = False
    source = File(dict(filename=old.filename + '.old'))(old)
    source.backup(new)
    old.readonly()


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
    store.validate()
