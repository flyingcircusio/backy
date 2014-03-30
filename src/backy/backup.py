from glob import glob
import backy
import hashlib
import json
import uuid
import time
import os


class Revision(object):

    uuid = None
    timestamp = None
    checksum = None
    parent = None
    blocks = None

    @classmethod
    def select_type(cls, type):
        if type == 'full':
            return FullRevision
        elif type == 'delta':
            return DeltaRevision
        raise ValueError('revision type %r unknown' % 'type')

    @classmethod
    def create(cls, type, backup):
        r = cls.select_type(type)(str(uuid.uuid4()), backup)
        r.timestamp = time.time()
        return r

    @classmethod
    def load(cls, file, backup):
        metadata = json.load(open(file))
        r = cls.select_type(metadata['type'])(
            metadata['uuid'], backup)
        r.timestamp = metadata['timestamp']
        r.parent = metadata['parent']
        r.checksum = metadata['checksum']
        r.blocks = dict(
            (int(i), data) for i, data in metadata['blocks'].items())
        return r

    def __init__(self, uuid, backup):
        self.uuid = uuid
        self.backup = backup

    @property
    def filename(self):
        return '{}/{}'.format(self.backup.path, self.uuid)

    @property
    def info_filename(self):
        return self.filename + '.backy'

    def write_info(self):
        metadata = {
            'uuid': self.uuid,
            'timestamp': self.timestamp,
            'checksum': self.checksum,
            'parent': self.parent,
            'blocks': self.blocks,
            'type': self.type}
        json.dump(metadata, open(self.info_filename, 'w'))

    def scrub(self, markbad=True):
        for i, chunk in self.iterchunks():
            if self.blocks[i].startswith('bad:'):
                print "Chunk {:06d} is known as corrupt.".format(i)
                continue
            if hashlib.md5(chunk).hexdigest() == self.blocks[i]:
                continue
            print "Chunk {:06d} is corrupt".format(i)
            self.blocks[i] = 'bad:{}'.format(self.blocks[i])
        if markbad:
            print "Marking corrupt chunks."
            self.write_info()


class FullRevision(Revision):

    type = 'full'

    _data = None

    def start(self, size):
        assert self._data is None
        # Prepare for storing data
        self._checksum = hashlib.md5()
        # XXX locking, assert it does not exist yet
        self._data = open(self.filename, 'w')

        self._data.seek(size)
        self._data.truncate()
        self._data.seek(0)

        self._index = 0

        self.blocks = {}

    def store(self, i, chunk):
        assert i == self._index
        assert len(chunk) == backy.CHUNKSIZE
        assert self._data.tell() == i*backy.CHUNKSIZE
        self._checksum.update(chunk)
        self._data.write(chunk)
        self.blocks[self._index] = hashlib.md5(chunk).hexdigest()
        self._index += 1

    def stop(self):
        self.checksum = self._checksum.hexdigest()
        self.write_info()

    def iterchunks(self):
        assert self._data is None
        self._data = open(self.filename, 'r')
        i = 0
        while True:
            chunk = self._data.read(backy.CHUNKSIZE)
            if not chunk:
                break
            yield i, chunk
            if len(chunk) != backy.CHUNKSIZE:
                break
            i += 1
        self._data.close()
        self._data = None

    def remove(self):
        os.unlink(self.filename)
        os.unlink(self.info_filename)


class DeltaRevision(Revision):

    type = 'delta'


class Source(object):

    _size = 0

    def __init__(self, filename):
        self.f = file(filename, "rb")
        # posix_fadvise(self.f.fileno(), 0, 0, POSIX_FADV_SEQUENTIAL)

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
        there = chunk_id * backy.CHUNKSIZE
        if there > self.size:
            return ""
        if here != there:
            self.f.seek(there)
        data = self.f.read(backy.CHUNKSIZE)
        # clear buffers for that action
        # XXX posix_fadvise(
        #     self.f.fileno(), 0, self.f.tell(), POSIX_FADV_DONTNEED)
        # XXX Stats().t(len(data))
        return data

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


class Backup(object):

    def __init__(self, path):
        # The path identifies the newest backup. Additional files
        # will be named with suffixes.
        self.path = path
        self.revisions = {}

        # Load all revision infos
        for file in glob(self.path + '/*.backy'):
            r = Revision.load(file, self)
            self.revisions[r.uuid] = r

    def backup(self, path):
        source = Source(path)
        r = Revision.create('full', self)
        r.start(source.size)
        for index, chunk in source.iterchunks():
            r.store(index, chunk)
        r.stop()
