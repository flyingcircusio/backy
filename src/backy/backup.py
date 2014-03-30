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
    blocksums = None
    blocks = None

    def __init__(self, uuid, backup):
        self.uuid = uuid
        self.backup = backup
        self.blocks = 0
        self.blocksums = {}

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
        r.blocksums = dict(
            (int(i), data) for i, data in metadata['blocksums'].items())
        r.blocks = metadata['blocks']
        return r

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
            'blocksums': self.blocksums,
            'blocks': self.blocks,
            'type': self.type}
        json.dump(metadata, open(self.info_filename, 'w'))

    def scrub(self, markbad=True):
        for i, chunk in self.iterchunks():
            if not i in self.blocksums:
                # XXX mark this revision as globally bad
                print "Unexpected block {:06d} found in data file.".format(i)
                continue
            if self.blocksums[i].startswith('bad:'):
                print "Chunk {:06d} is known as corrupt.".format(i)
                continue
            if hashlib.md5(chunk).hexdigest() == self.blocksums[i]:
                continue
            print "Chunk {:06d} is corrupt".format(i)
            self.blocksums[i] = 'bad:{}'.format(self.blocksums[i])
        if markbad:
            print "Marking corrupt chunks."
            self.write_info()

    def remove(self):
        os.unlink(self.filename)
        os.unlink(self.info_filename)

    def restore(self, target):
        assert not target.startswith(self.backup.path)

        target = open(target, 'wb')
        checksum = hashlib.md5()
        for i, chunk in self.iterchunks(True):
            checksum.update(chunk)
            print "{:06d} | - | RESTORE".format(i)
            target.write(chunk)
        if checksum.hexdigest() != self.checksum:
            print "WARNING: restored with inconsistent checksum."
        else:
            print "Restored with matching checksum."
        os.fsync(target.fileno())
        target.close()


class FullRevision(Revision):

    type = 'full'

    _data = None
    delta = None

    def start(self, size):
        assert self._data is None
        # Prepare for storing data
        self._checksum = hashlib.md5()
        # XXX locking, assert it does not exist yet
        try:
            self._data = open(self.filename, 'rb+')
        except Exception:
            self._data = open(self.filename, 'wb+')

        self._data.seek(size)
        self._data.truncate()
        self._data.seek(0)

        self._seen_last_chunk = False
        self.blocks = 0

        print "Starting to back up revision {}".format(self.uuid)
        if self.delta:
            print "\t using delta revision {}".format(self.delta.uuid)
            self.delta.start()

    def store(self, i, chunk):
        assert i == self.blocks
        assert not self._seen_last_chunk

        if len(chunk) != backy.CHUNKSIZE:
            self._seen_last_chunk = True

        checksum = hashlib.md5(chunk).hexdigest()
        self._checksum.update(chunk)
        print "{:06d} | {} | PROCESS".format(i, checksum)

        if self.blocksums.get(i) != checksum:
            if self.delta:
                self._data.seek(i*backy.CHUNKSIZE)
                print "{:06d} | {} | STORE DELTA".format(i, self.blocksums[i])
                old_chunk = self._data.read(backy.CHUNKSIZE)
                self.delta.store(i, old_chunk)
                self.delta.blocksums[i] = self.blocksums[i]

            print "{:06d} | {} | STORE MAIN".format(i, checksum)
            self._data.seek(i*backy.CHUNKSIZE)
            self._data.write(chunk)
            self.blocksums[i] = checksum

        self.blocks += 1

    def stop(self):
        self._data.close()
        self._data = None
        self.checksum = self._checksum.hexdigest()
        self.write_info()
        if self.delta:
            self.delta.stop()

    def iterchunks(self, full=True):
        assert self._data is None
        self._data = open(self.filename, 'rb')
        i = 0
        while True:
            chunk = self._data.read(backy.CHUNKSIZE)
            if not chunk:
                break
            yield i, chunk
            if len(chunk) != backy.CHUNKSIZE:
                break
            i += 1
        if not i == self.blocks:
            print "READ {} blocks instead if {}".format(i, self.blocks)
        self._data.close()
        self._data = None

    def getChunk(self, i):
        f = open(self.filename, 'rb')
        f.seek(i*backy.CHUNKSIZE)
        # XXX check against stored checksum
        return f.read(backy.CHUNKSIZE)

    def migrate_to_delta(self):
        full = Revision.create('full', self.backup)

        previous = DeltaRevision(self.uuid, self.backup)
        previous.timestamp = self.timestamp
        previous.checksum = self.checksum
        previous.parent = full.uuid
        previous.blocks = self.blocks
        previous.write_info()

        os.link(self.filename, full.filename)
        os.unlink(self.filename)

        full.delta = previous
        full.blocksums = self.blocksums

        return full


class DeltaRevision(Revision):

    type = 'delta'

    _data = None

    def start(self):
        self._data = open(self.filename, 'wb')

    def store(self, i, chunk):
        self.blocksums[i] = hashlib.md5(chunk).hexdigest()
        self._data.write(chunk)

    def stop(self):
        self._data.close()
        self._data = None
        self.write_info()

    def iterchunks(self, full=False):
        if full:
            blocks = xrange(self.blocks)
        else:
            blocks = sorted(self.blocksums.keys())

        for i in blocks:
            yield i, self.getChunk(i)

    def getChunk(self, i):
        # Calling getchunk often seems wasteful.
        # XXX check against stored checksum
        if not i in self.blocksums:
            return self.backup.revisions[self.parent].getChunk(i)
        f = open(self.filename, 'rb')
        blocksums = self.blocksums.keys()
        blocksums.sort()
        f.seek(blocksums.index(i) * backy.CHUNKSIZE)
        return f.read(backy.CHUNKSIZE)


class Source(object):

    _size = 0

    def __init__(self, filename):
        self.f = file(filename, "rb")
        # posix_fadvise(self.f.fileno(), 0, 0, POSIX_FADV_SEQUENTIAL)

    def iterchunks(self):
        i = 0
        self.f.seek(0)
        while True:
            data = self.f.read(backy.CHUNKSIZE)
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

        self.revision_history = self.revisions.values()
        self.revision_history.sort(key=lambda r: r.timestamp)

    def backup(self, path):
        source = Source(path)

        if self.revision_history:
            previous = self.revision_history[-1]
            r = previous.migrate_to_delta()
        else:
            r = Revision.create('full', self)

        r.start(source.size)
        for index, chunk in source.iterchunks():
            r.store(index, chunk)
        r.stop()

        if os.path.exists(self.path+'/last'):
            os.unlink(self.path+'/last')
        os.symlink(r.filename, self.path+'/last')

        if os.path.exists(self.path+'/last.backy'):
            os.unlink(self.path+'/last.backy')
        os.symlink(r.info_filename, self.path+'/last.backy')
