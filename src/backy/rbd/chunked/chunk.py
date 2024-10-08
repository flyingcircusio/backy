import binascii
import io
import os
import tempfile
from typing import TYPE_CHECKING, Optional, Tuple

import lzo
import mmh3

from backy.utils import posix_fadvise

from . import BackendException, Hash, InconsistentHash

if TYPE_CHECKING:
    from .store import Store


class Chunk(object):
    """A chunk in a file that represents a part of it.

    This can be read and updated in memory, which updates its
    hash and then can be flushed to disk when appropriate.

    """

    CHUNK_SIZE = 4 * 1024**2  # 4 MiB chunks

    hash: Optional[Hash]
    store: "Store"
    clean: bool
    data: Optional[io.BytesIO]
    stats: dict

    def __init__(
        self, store: "Store", hash: Optional[Hash], stats: Optional[dict] = None
    ):
        self.hash = hash
        self.store = store
        self.clean = True
        self.data = None
        self.stats = stats if stats is not None else dict()

    def _read_existing(self) -> None:
        if self.data:
            return
        # Prepare working with the chunk. We keep the data in RAM for
        # easier random access combined with transparent compression.
        data = b""
        if self.hash:
            chunk_file = self.store.chunk_path(self.hash)
            try:
                with open(chunk_file, "rb") as f:
                    posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)  # type: ignore
                    data = f.read()
                    data = lzo.decompress(data)
            except (lzo.error, IOError) as e:
                raise BackendException from e

            disk_hash = hash(data)
            # This is a safety belt. Hashing is sufficiently fast to avoid
            # us accidentally reading garbage.
            if disk_hash != self.hash:
                raise InconsistentHash(self.hash, disk_hash)
        self._init_data(data)

    def _init_data(self, data: bytes) -> None:
        self.data = io.BytesIO(data)

    def read(self, offset: int, size: int = -1) -> Tuple[bytes, int]:
        """Read data from the chunk.

        Return the data and the remaining size that should be read.
        """
        self._read_existing()
        assert self.data

        self.data.seek(offset)
        data = self.data.read(size)
        remaining = -1
        if size != -1:
            remaining = max([0, size - len(data)])
        return data, remaining

    def write(self, offset: int, data: bytes) -> Tuple[int, bytes]:
        """Write data to the chunk, returns

        - the amount of data we used
        - the _data_ remaining

        """
        remaining_data = data[self.CHUNK_SIZE - offset :]
        data = data[: self.CHUNK_SIZE - offset]

        if offset == 0 and len(data) == self.CHUNK_SIZE:
            # Special case: overwrite the entire chunk.
            self._init_data(data)
            self.stats.setdefault("write_full", 0)
            self.stats["write_full"] += 1
        else:
            self._read_existing()
            assert self.data
            self.data.seek(offset)
            self.data.write(data)
            self.stats.setdefault("write_partial", 0)
            self.stats["write_partial"] += 1
        self.clean = False

        return len(data), remaining_data

    def flush(self) -> Optional[Hash]:
        """Writes data to disk if necessary
        Returns the new Hash on updates
        """
        if self.clean:
            return None
        assert self.data
        # I'm not using read() here to a) avoid cache accounting and b)
        # use a faster path to get the data.
        self.hash = hash(self.data.getvalue())
        target = self.store.chunk_path(self.hash)
        if self.hash not in self.store.seen:
            if self.store.force_writes or not target.exists():
                # Create the tempfile in the right directory to increase
                # locality of our change - avoid renaming between multiple
                # directories to reduce traffic on the directory nodes.
                fd, tmpfile_name = tempfile.mkstemp(dir=target.parent)
                posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)  # type: ignore
                with os.fdopen(fd, mode="wb") as f:
                    data = lzo.compress(self.data.getvalue())
                    f.write(data)
                # Micro-optimization: chmod before rename to help against
                # metadata flushes and then changing metadata again.
                os.chmod(tmpfile_name, 0o440)
                os.rename(tmpfile_name, target)
            self.store.seen.add(self.hash)
        self.clean = True
        return self.hash


def hash(data: bytes) -> Hash:
    return binascii.hexlify(mmh3.hash_bytes(data)).decode("ascii")
