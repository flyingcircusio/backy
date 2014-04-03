import hashlib
from subprocess import check_call

cmd = check_call


class Source(object):

    _size = 0

    type_ = 'plain'

    def __init__(self, filename, backup):
        self.filename = filename
        self.backup = backup
        # posix_fadvise(self.f.fileno(), 0, 0, POSIX_FADV_SEQUENTIAL)

    def open(self):
        self.f = file(self.filename, "rb")

    def close(self):
        self.f.close()

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


class CephSource(object):

    # The filename is expected to hold "pool/volume".

    type_ = 'ceph-rbd'

    def open(self):
        self.ceph_volume = self.filename
        self.filename = '/dev/rbd/{}@backy'

        cmd('rbd snap create {}@backy'.format(self.ceph_volume))
        cmd('rbd map {}@backy'.format(self.ceph_volume))

        super(CephSource, self).prepare()

    def close(self):
        cmd('rbd unmap {}@backy'.format(self.ceph_volume))
        cmd('rbd snap rm {}@backy'.format(self.ceph_volume))
        super(CephSource, self).close()
