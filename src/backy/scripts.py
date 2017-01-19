import argparse
from backy.backup import Backup
from backy.source.file import File
import os


def file2chunk():
    """A very lightweight script to manually convert revisions to a
    chunk store."""
    parser = argparse.ArgumentParser(
        description='Convert a revision from CoW file to chunked.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('revision')
    args = parser.parse_args()

    # Rename the old data file so that we can use it as a new input
    # and re-create the old revision.
    backup = Backup('.')
    old = backup.find(args.revision)
    os.rename(old.filename, old.filename + '.old')
    old.writable()
    new = backup.backend_factory(old)
    source = File(dict(filename=old.filename + '.old'))(old)
    source.backup(new)
