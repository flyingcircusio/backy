import backy
import glob
import gzip
import os


class Differ(object):
    """ finds existing diffs, creates a new one and feeds it
        with data.
    """

    index = None
    finished = ""
    filename = ""

    def __init__(self, backupfile, chunksize=backy.CHUNKSIZE, old_mtime=None):
        """ prefix is a path with a file prefix for the diffs like this:
                /backup/server/out.diff
            The real diff files will be
                /backup/server/out.diff.1
                /backup/server/out.diff.2
                ...

            The diff-index-file contains:
            - was the backup procedure finished?
            - the chunk size with which the diff was created
            - a table of changed chunks: chunk_id, offset, length

            The diff-file contains the following information:
            - the changed chunks or parts of them gzipped
        """
        prefix = backupfile + '.diff'
        self.index = []
        self.chunksize = chunksize

        max_diff = 0
        diffs = sorted(glob.glob("%s.*" % prefix), reverse=True)
        diffs = [
            d for d in diffs
            if not d.endswith(".index") and not d.endswith(".md5")]
        if len(diffs) > 0:
            # XXX check and raise if the diff looks strange?
            max_diff = int(diffs[0].split(".")[-1])
        self.filename = "%s.%08d" % (prefix, max_diff+1)
        self.index_filename = "%s.%08d.index" % (prefix, max_diff+1)
        self.f = file(self.filename, "wb")
        self.old_mtime = old_mtime

    def dump(self):
        yield "%s\n" % self.finished
        yield "%s\n" % self.chunksize
        for i in self.index:
            yield "%d,%d,%d\n" % i  # chunk_id, offset, length

    def write_index(self):
        data = self.dump()
        file(self.index_filename, "w").writelines(data)

    def setChunk(self, chunk_id, data, compression_level=1):
        z = gzip.zlib.compress(data, compression_level)
        offset = self.f.tell()
        length = len(z)
        self.index.append((chunk_id, offset, length))
        self.write_index()
        self.f.write(z)
        # XXX posix_fadvise(self.f.fileno(), 0, self.f.tell(),
        #    POSIX_FADV_DONTNEED)
        # XXX Stats().t(len(z))

    def close(self):
        self.finished = "Successfully finished."
        self.write_index()
        self.f.close()
        # set old mtime
        if self.old_mtime:
            os.utime(self.filename, (self.old_mtime, self.old_mtime))
            os.utime(self.index_filename, (self.old_mtime, self.old_mtime))
