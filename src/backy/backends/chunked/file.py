from .chunk import Chunk
import io
import json
import os.path


class File(object):
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

    def __init__(self, name, store, mode='rw'):
        self.name = name
        self.store = store
        self.closed = False
        self._position = 0

        if '+' in mode:
            mode += 'w'
        self.mode = mode

        if not os.path.exists(name) and 'w' not in mode:
            raise RuntimeError('File not found: {}'.format(self.name))

        if not os.path.exists(name):
            self._mapping = {}
            self.size = 0
        else:
            # The mapping stores its chunk IDs as strings, because JSON can't
            # have ints as keys. We convert them explicitly back to integers
            # because we keep computing with them.
            with open(self.name, 'r') as f:
                meta = json.load(f)
                self._mapping = {int(k): v for k, v in meta['mapping'].items()}
                self.size = meta['size']

        if '+' in mode:
            self._position = self.size

        # Chunks that we are working on.
        self._chunks = {}

    def fileno(self):
        raise OSError('ChunkedFile does not support use through a file '
                      'descriptor.')

    def flush(self):
        assert 'w' in self.mode and not self.closed
        for chunk_id in self._chunks.copy():
            chunk = self._chunks.pop(chunk_id)
            chunk.flush()
        with open(self.name, 'w') as f:
            json.dump({'mapping': self._mapping,
                       'size': self.size}, f)

    def close(self):
        assert not self.closed
        if 'w' in self.mode:
            self.flush()
        self.closed = True

    def isatty(self):
        return False

    def readable(self):
        return 'r' in self.mode and not self.closed

    # def readline(size=-1)
    # def readlines(hint=-1)

    def tell(self):
        assert not self.closed
        return self._position

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        assert not self.closed

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

            self.size = position
        self._position = position
        return position

    def truncate(self, size=None):
        assert 'w' in self.mode and not self.closed
        if size is None:
            size = self._position
        # Update content hash
        self.size = size
        # Remove chunks past the size
        to_remove = set(
            key for key in self._mapping if key * Chunk.CHUNK_SIZE > size)
        for key in to_remove:
            del self._mapping[key]
        for key in self._chunks:
            del self._chunks[key]
        self.flush()

    def read(self, size=-1):
        assert 'r' in self.mode and not self.closed
        result = io.BytesIO()
        max_size = self.size - self._position
        if size == -1:
            size = max_size
        else:
            size = min([size, max_size])
        while size:
            chunk, offset = self._current_chunk()
            data, size = chunk.read(offset, size)
            self._position += len(data)
            result.write(data)
        return result.getvalue()

    def writable(self):
        return 'w' in self.mode and not self.closed

    def write(self, data):
        assert 'w' in self.mode and not self.closed
        while data:
            chunk, offset = self._current_chunk()
            written, data = chunk.write(offset, data)
            self._position += written
            if self._position > self.size:
                self.size = self._position

    def _current_chunk(self):
        chunk_id = self._position // Chunk.CHUNK_SIZE
        offset = self._position % Chunk.CHUNK_SIZE
        if chunk_id not in self._chunks:
            self._chunks[chunk_id] = Chunk(
                self, chunk_id, self.store, self._mapping.get(chunk_id))
        return self._chunks[chunk_id], offset

    def __enter__(self):
        assert not self.closed
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.close()
