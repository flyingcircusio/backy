import fcntl
import os
import os.path
import threading


class LockedFile(object):

    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode

    def __enter__(self):
        self.fh = open(self.filename, self.mode)
        fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return self.fh

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._fh.flush()
        os.fsync(self._fh)
        self.fh.close()


class ChunkWriter(object):
    """Provide a context manager API for writing to a chunk store.
    """

    def __init__(self, filename, chunksize, size=None):
        self.filename = filename
        self.chunksize = chunksize
        self.size = size
        self._lock = threading.Lock()

    def _write_enable(self):
        if os.path.exists(self.filename):
            os.chmod(self.filename, 0o640)

    def _write_protect(self):
        os.chmod(self.filename, 0o440)

    def __enter__(self):
        # The context manager is not intended to be re-entrant and we
        # must protect against a store being put into write mode multiple
        # times.
        # This is a soft improvement around the file lock that avoids
        # internal data corruption (e.g. the file handle being accidentally
        # replaced).
        if not self._lock.acquire(False):
            raise Exception("ChunkWriter already locked.")

        # Existing stores are expected to have been marked
        # as read-only after finishing writing to it. Before
        # writing again we need to mark it writable.
        self._write_enable()

        # If the store already exists, re-use the existing file
        # in the hope to reduce on-disk fragmentation.
        try:
            self._data = open(self.filename, 'rb+')
        except Exception:
            self._data = open(self.filename, 'wb+')

        # Chunk files are expected to not be accidentally written to so we turn
        # it into read-only mode right after creating the file handle.
        self._write_protect()

        # Ensure that nobody else (who respects the lock) starts writing
        # to the
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        # There's two inter-locking race conditions between making the file
        # writable and getting the lock that can cause the file being left
        # unprotected. It's safe to just do it twice.
        self._write_protect()

        if self.size:
            self._fh.seek(self.size)
            self._fh.truncate()
            self._fh.seek(0)

        # This is the "incomplete chunk" marker: a single chunk may be of a
        # size smaller than the given chunk size, which indicates that we're at
        # the end of the file.
        self._seen_incomplete_chunk = False
        return self

    def store(self, i, chunk):
        """Store a chunk.

        i is the local index in the store.

        The assumption is that, generally, all blocks that are written
        are stored in order, sequentially.

        The last chunk may be incomplete.

        """
        if self._seen_incomplete_chunk:
            raise RuntimeError(
                "Can not store additional chunks after an incomplete chunk. "
                "Must stop.")

        if not len(chunk) <= self.chunksize:
            raise ValueError(
                "Can not store chunks larger than the configured chunk size:"
                " {} > {}".format(len(chunk), self.chunksize))

        if len(chunk) < self.chunksize:
            self._seen_incomplete_chunk = True

        self._fh.seek(i * self.chunksize)
        self._fh.write(chunk)

    def __exit__(self, exc_type, exc_value, exc_tb):
        # Ensure all data is actually persisted.
        self._fh.flush()
        os.fsync(self._fh)

        # If we re-used the file then we may not be at the end right now. Mark
        # this as the end to ensure we do not read residual data in the future.
        self._fh.truncate()

        self._fh.close()
        self._fh = None

        self._lock.release()


class ChunkStore(object):
    """Low-level file format abstraction.

    Does not know about full/delta images and does not care about
    higher-level chunk IDs: IDs are only local to this one store.

    """

    def __init__(self, filename, chunksize):
        self.filename = filename
        self.chunksize = chunksize

    def writer(self, size=None):
        return ChunkWriter(self.filename, self.chunksize, size)

    def iterChunks(self):
        with LockedFile(self.filename, 'rb') as fh:
            i = 0
            while True:
                chunk = fh.read(self.CHUNKSIZE)
                if not chunk:
                    break
                yield i, chunk
                i += 1

    def getChunk(self, i):
        with LockedFile(self.filename, 'rb') as fh:
            fh.seek(i * self.chunksize)
            return fh.read(self.chunksize)
