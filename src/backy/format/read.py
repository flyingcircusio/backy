import backy
# from fallocate import posix_fadvise, POSIX_FADV_SEQUENTIAL


class Reader(object):

    _size = 0

    def __init__(self, filename, chunksize=backy.CHUNKSIZE):
        self.f = file(filename, "rb")
        # posix_fadvise(self.f.fileno(), 0, 0, POSIX_FADV_SEQUENTIAL)
        self.chunksize = chunksize

    def iterchunks(self):
        i = 0
        while True:
            data = self.getChunk(i)
            if not data:
                break
            yield i, data
            i += 1

    def getChunk(self, chunk_id):
        here = self.f.tell()
        there = chunk_id * self.chunksize
        if there > self.size():
            return ""
        if here != there:
            self.f.seek(there)
        data = self.f.read(self.chunksize)
        # clear buffers for that action
        # XXX posix_fadvise(
        #     self.f.fileno(), 0, self.f.tell(), POSIX_FADV_DONTNEED)
        # XXX Stats().t(len(data))
        return data

    def size(self):
        # returns the size in bytes.
        if self._size:
            return self._size
        here = self.f.tell()
        self.f.seek(0, 2)
        size = self.f.tell()
        self.f.seek(here)
        self._size = size
        return size

    def close(self):
        self.f.close()
