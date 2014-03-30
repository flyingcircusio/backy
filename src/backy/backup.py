import os


class Revision(object):

    def __init__(self, backup, revision):
        self.backup = backup
        self.name = revision


class FullRevision(Revision):

    @property
    def size(self):
        stat = os.stat(self.backup.path)
        return stat.st_size

    @property
    def timestamp(self):
        stat = os.stat(self.backup.path)
        return stat.st_mtime


class Backup(object):

    def __init__(self, path):
        # The path identifies the newest backup. Additional files
        # will be named with suffixes.
        self.path = path

        self.revisions = {}
        self.revisions['0'] = FullRevision(self, '0')
