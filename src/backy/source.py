import hashlib


class Source(object):

    _size = 0

    def __init__(self, filename, backup):
        self.f = file(filename, "rb")
        self.backup = backup
        # posix_fadvise(self.f.fileno(), 0, 0, POSIX_FADV_SEQUENTIAL)

    def iterchunks(self):
        i = 0
        self.f.seek(0)
        while True:
            data = self.f.read(self.backup.CHUNKSIZE)
            if not data:
                break
            print "{:06d} | {} | READ".format(
                i, hashlib.md5(data).hexdigest())
            yield i, data
            i += 1

    @property
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
