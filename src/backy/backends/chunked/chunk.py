import hashlib
import os
import tempfile
import gzip
import io


class Chunk(object):
    """A chunk in a file that represents a part of it.

    This can be read and updated in memory, which updates its
    hash and then can be flushed to disk when appropriate.

    """

    CHUNK_SIZE = 4 * 1024**2  # 4 MiB chunks

    def __init__(self, file, id, store, hash):
        self.id = id
        self.hash = hash
        self.file = file
        self.store = store
        self.clean = True

        # Prepare working with the chunk. We keep the data in RAM for
        # easier random access combined with transparent compression.
        data = b''

        # We may have an existing file that is uncompressed.
        if self.hash:
            raw = self.store.chunk_path(self.hash, compressed=False)
            compressed = self.store.chunk_path(self.hash)
            if os.path.exists(raw):
                with open(raw, 'rb') as f:
                    data = f.read()
                self.clean = False
            elif os.path.exists(compressed):
                with gzip.open(compressed, 'rb') as f:
                    data = f.read()

        self.data = io.BytesIO(data)

    def read(self, offset, size=-1):
        """Read data from the chunk.

        Return the data and the remaining size that should be read.
        """
        # We can always only do once. To support some edge cases we support
        # switching transparently between those modes.
        self.data.seek(offset)
        data = self.data.read(size)
        remaining = -1
        if size != -1:
            remaining = max([0, size - len(data)])
        return data, remaining

    def write(self, offset, data):
        """Write data to the chunk, returns

        - the amount of data we used
        - the _data_ remaining

        """
        remaining_data = data[self.CHUNK_SIZE - offset:]
        data = data[:self.CHUNK_SIZE - offset]

        self.data.seek(offset)
        self.data.write(data)
        self.clean = False

        return len(data), remaining_data

    def flush(self):
        if self.clean:
            return
        self._update_hash()
        target = self.store.chunk_path(self.hash)
        if not os.path.exists(target):
            fd, tmpfile_name = tempfile.mkstemp(dir=self.store.path)
            with gzip.open(tmpfile_name, mode='wb', compresslevel=1) as f:
                f.write(self.data.getvalue())
            os.rename(tmpfile_name, target)
            os.chmod(target, 0o440)
        uncompressed = self.store.chunk_path(self.hash, compressed=False)
        if os.path.exists(uncompressed):
            os.unlink(uncompressed)
        self.clean = True

    def _update_hash(self):
        data, _ = self.read(0)
        self.hash = hashlib.new('sha256', data).hexdigest()
        self.file._mapping[self.id] = self.hash
