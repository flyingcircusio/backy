import hashlib
import os
import tempfile


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
        self.read_file = None
        self.write_file = None
        self.write_file_name = None

    def read(self, offset, size=-1):
        """Read data from the chunk.

        Return the data and the remaining size that should be read.
        """
        if not self.read_file:
            self.read_file = open(self.store.chunk_path(self.hash), 'rb')
        self.read_file.seek(offset)
        data = self.read_file.read(size)
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
        if self.write_file is None:
            fd, self.write_file_name = tempfile.mkstemp(dir=self.store.path)
            self.write_file = os.fdopen(fd, mode='a+b')
            if self.hash:
                self.write_file.write(self.read(0)[0])
            self.read_file = self.write_file

        self.write_file.seek(offset)
        self.write_file.write(data)
        self.clean = False

        return len(data), remaining_data

    def flush(self):
        if not self.write_file:
            return
        self._update_hash()
        self.write_file.close()
        target = self.store.chunk_path(self.hash)
        if not os.path.exists(target):
            os.rename(self.write_file_name, target)
            os.chmod(target, 0o440)
        else:
            os.unlink(self.write_file_name)
        self.write_file = None
        self.write_file_name = None
        self.read_file = None
        self.clean = True

    def _update_hash(self):
        data, _ = self.read(0)
        self.hash = hashlib.new('sha256', data).hexdigest()
        self.file._mapping[self.id] = self.hash
