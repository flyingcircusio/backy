import glob
import hashlib
import io
import json
import os.path
import tempfile

# Support validating chunks in a store.

# Support not flushing intermediate chunks until a 4MiB is reached or the file
# is closed to avoid too many chunks that will immediately become obsolete.

# A chunkstore, is responsible for all revisions for a single backup, for now.
# We can start having statistics later how much reuse between images is
# actually there and maybe extend this to multiple images. This then requires a
# lot more coordination about the users of the pool.


class ChunkStore(object):

    def __init__(self, path='/tmp/chunks'):
        self.path = path
        self.users = []
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def validate(self):
        for file in glob.glob(self.path + '/*.chunk'):
            file_hash = os.path.splitext(os.path.split(file)[1])[0]
            data = open(file, 'rb').read()
            hash = hashlib.new('sha256', data).hexdigest()
            assert file_hash == hash, "Content mismatch for {}".format(hash)

    def expunge(self):
        # Need to hold a global lock. May want to limit operations holding
        # the lock to a certain number of operations and then release the lock
        # so that others can write again. After that we would have to get
        # the lock again and then reload the list of our users.
        used_hashes = set()
        for user in self.users:
            used_hashes.update(user._mapping.values())

        for file in glob.glob(self.path + '/*.chunk'):
            file_hash = os.path.splitext(os.path.split(file)[1])[0]
            if file_hash not in used_hashes:
                os.unlink(file)

    def chunk_path(self, hash):
        return os.path.join(self.path, hash + '.chunk')


class ChunkedFile(object):
    """A file like class that stores its data in 4MiB chunks
    identified by their hashes.

    It keeps a mapping of offset -> hash.

    When a file is modified then the mapping is updated to the new hash.

    The actual filename only identifies the file that keeps the mapping
    which is written out when calling flush().

    It currently only supports a simplified generic read/write mode.

    Individual chunks may be smaller than 4MiB as they maybe be at file
    boundaries.

    """

    CHUNK_SIZE = 4 * 1024**2  # 4 MiB chunks

    def __init__(self, name, store, mode='rw'):
        self.name = name
        self.store = store
        self.closed = False
        self._position = 0

        if not os.path.exists(name):
            # The mapping will have integers represented as strings as its
            # keys -> json doesn't support integer keys.
            self._mapping = {}
            self.size = 0
        else:
            with open(self.name, 'r') as f:
                meta = json.load(f)
                self._mapping = meta['mapping']
                self.size = meta['size']

    def fileno(self):
        raise OSError('ChunkedFile does not support use through a file '
                      'descriptor.')

    def flush(self):
        with open(self.name, 'w') as f:
            json.dump({'mapping': self._mapping,
                       'size': self.size}, f)

    def close(self):
        self.flush()
        self.closed = True

    def isatty(self):
        return False

    def readable(self):
        return True

    # def readline(size=-1)
    # def readlines(hint=-1)

    def seek(self, offset, whence=io.SEEK_SET):
        position = self._position
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_END:
            position = self.size - offset
        elif whence == io.SEEK_CUR:
            position = position + whence
        else:
            raise ValueError(
                '`whence` does not support mode {}'.format(whence))

        if position < 0:
            raise ValueError('Can not seek before the beginning of a file.')
        if position > self.size:
            # XXX update the position map?
            self.size = position
        self._position = position
        return position

    def seekable(self):
        return True

    def tell(self):
        return self._position

    def truncate(self, size=None):
        if size is None:
            size = self.position
        # Update content hash
        self.size = size

    def writable(self):
        return True

    def read(self, size=-1):
        assert size == -1, 'Limited reads not implemented yet'
        result = b''
        while self._position < self.size:
            chunk, offset = self._current_chunk_offset()

            chunk_hash = self._mapping.get(str(chunk), None)
            if not chunk_hash:
                # End of file
                break

            with open(self.store.chunk_path(chunk_hash), 'rb') as f:
                f.seek(offset)
                chunk_data = f.read()

            self._position += len(chunk_data)
            result += chunk_data

        return result

    def write(self, b):
        while b:
            chunk, offset = self._current_chunk_offset()

            new_part = b[:self.CHUNK_SIZE - offset]

            chunk_hash = self._mapping.get(str(chunk), None)
            if not chunk_hash:
                chunk_data = b'\0' * (offset + len(new_part))
            else:
                chunk_data = open(
                    self.store.chunk_path(chunk_hash), 'rb').read()
            new_chunk_data = (chunk_data[:offset] +
                              new_part +
                              chunk_data[offset + len(new_part):])

            new_chunk_hash = hashlib.new('sha256', new_chunk_data).hexdigest()
            target = self.store.chunk_path(new_chunk_hash)
            if not os.path.exists(target):
                # We assume that if the chunked hash already exists, everything
                # will be fine as we do atomic updates.
                _, tmp_target = tempfile.mkstemp(dir=self.store.path)
                open(tmp_target, 'wb').write(new_chunk_data)
                os.rename(tmp_target, target)

            self._position += len(new_part)
            if self._position > self.size:
                self.size = self._position
            self._mapping[str(chunk)] = new_chunk_hash

            b = b[self.CHUNK_SIZE - offset:]

    def _current_chunk_offset(self):
        chunk = self._position // self.CHUNK_SIZE
        offset = self._position % self.CHUNK_SIZE
        return chunk, offset

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class ChunkedFileBackend(object):

    def __init__(self, revision):
        self.revision = revision
        self.store = ChunkStore(path=self.revision.archive.path + '/chunks')

    def open(self, mode='rb'):
        return ChunkedFile(self.revision.filename, mode, store=self.store)
