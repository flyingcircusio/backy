import io
import json
import os
import os.path
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Optional, Tuple

from .chunk import Chunk, Hash

if TYPE_CHECKING:
    from backy.rbd.chunked import Store


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

    flush_target = 10

    name: str
    store: "Store"
    closed: bool
    overlay: bool
    size: int
    mode: str

    _position: int
    _access_stats: dict[int, Tuple[int, float]]  # (count, last)
    _mapping: dict[int, Hash]
    _chunks: dict[int, Chunk]

    def __init__(
        self,
        name: str | os.PathLike,
        store: "Store",
        mode: str = "rw",
        overlay: bool = False,
    ):
        self.name = str(name)
        self.store = store
        self.closed = False
        # This indicates that writes should be temporary and no modify
        # the metadata when closing.
        self.overlay = overlay
        self._position = 0

        self._access_stats = defaultdict(lambda: (0, 0))

        self.mode = mode

        if "+" in self.mode:
            self.mode += "w"
        if "a" in self.mode:
            self.mode += "w"
        self.mode = "".join(set(self.mode))

        if not os.path.exists(name) and "w" not in self.mode:
            raise FileNotFoundError("File not found: {}".format(self.name))

        if not os.path.exists(name):
            self._mapping = {}
            self.size = 0
        else:
            # The mapping stores its chunk IDs as strings, because JSON can't
            # have ints as keys. We convert them explicitly back to integers
            # because we keep computing with them.
            with open(self.name, "r") as f:
                # Safeguard: Make sure the file looks like json.
                if f.read(2) != '{"':
                    raise ValueError(
                        "Revision does not look like it's chunked."
                    )
                f.seek(0)
                meta = json.load(f)
                self._mapping = {int(k): v for k, v in meta["mapping"].items()}
                self.size = meta["size"]

        if "a" in self.mode:
            self._position = self.size

        # Chunks that we are working on.
        self._chunks = {}

    def fileno(self) -> int:
        raise OSError(
            "ChunkedFile does not support use through a file descriptor."
        )

    def _flush_chunks(self, target: Optional[int] = None) -> None:
        target = target if target is not None else self.flush_target
        if len(self._chunks) < (2 * target):
            return

        chunks = list(self._chunks.items())
        chunks.sort(key=lambda i: self._access_stats[i[0]], reverse=True)

        keep_chunks = chunks[:target]
        remove_chunks = chunks[target:]

        for id, chunk in remove_chunks:
            hash = chunk.flush()
            if hash:
                self._mapping[id] = hash

        self._chunks = dict(keep_chunks)

    def flush(self) -> None:
        assert "w" in self.mode and not self.closed

        self._flush_chunks(0)

        if not self.overlay:
            with open(self.name, "w") as f:
                json.dump({"mapping": self._mapping, "size": self.size}, f)
                f.flush()
                os.fsync(f)

    def close(self) -> None:
        assert not self.closed
        if "w" in self.mode:
            self.flush()
        self.closed = True

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return "r" in self.mode and not self.closed

    # def readline(size=-1)
    # def readlines(hint=-1)

    def tell(self) -> int:
        assert not self.closed
        return self._position

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence=io.SEEK_SET) -> int:
        assert not self.closed

        position = self._position
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_END:
            position = self.size - offset
        elif whence == io.SEEK_CUR:
            position = position + offset
        else:
            raise ValueError(
                "`whence` does not support mode {}".format(whence)
            )

        if position < 0:
            raise ValueError("Can not seek before the beginning of a file.")
        if position > self.size:
            # Fill up the missing parts with zeroes.
            target = position
            self.seek(self.size)
            # This is not performance optimized at all. We could do some
            # lower-level magic to avoid copying data all over all the time.
            # Ideally we would fill up the last existing chunk and then
            # just inject the well-known "zero chunk" into the map.
            filler = position - self.size
            while filler > 0:
                chunk = min(filler, 4 * 1024 * 1024) * b"\00"
                filler = filler - len(chunk)
                self.write(chunk)

            # Filling up should have caused our position to have moved properly
            # and the size also reflecting this already.
            assert self._position == target
            assert self.size == target

        self._position = position
        return position

    def truncate(self, size: Optional[int] = None) -> None:
        assert "w" in self.mode and not self.closed
        if size is None:
            size = self._position
        # Update content hash
        self.size = size
        # Remove chunks past the size
        to_remove = set(
            key for key in self._mapping if key * Chunk.CHUNK_SIZE > size
        )
        for key in to_remove:
            del self._mapping[key]
        self.flush()

    def read(self, size: int = -1) -> bytes:
        assert "r" in self.mode and not self.closed
        result = io.BytesIO()
        max_size = self.size - self._position
        if size == -1:
            size = max_size
        else:
            size = min([size, max_size])
        while size:
            chunk, id, offset = self._current_chunk()
            data, size = chunk.read(offset, size)
            if not data:
                raise ValueError(
                    f"Under-run: chunk {id} seems to be missing data"
                )
            self._position += len(data)
            result.write(data)
        return result.getvalue()

    def writable(self) -> bool:
        return "w" in self.mode and not self.closed

    def write(self, data: bytes) -> None:
        assert "w" in self.mode and not self.closed
        while data:
            chunk, _, offset = self._current_chunk()
            written, data = chunk.write(offset, data)
            self._position += written
            if self._position > self.size:
                self.size = self._position

    def _current_chunk(self) -> Tuple[Chunk, int, int]:
        chunk_id = self._position // Chunk.CHUNK_SIZE
        offset = self._position % Chunk.CHUNK_SIZE
        if chunk_id not in self._chunks:
            self._flush_chunks()
            self._chunks[chunk_id] = Chunk(
                self.store, self._mapping.get(chunk_id)
            )
        count = self._access_stats[chunk_id][0]
        self._access_stats[chunk_id] = (count + 1, time.time())
        return self._chunks[chunk_id], chunk_id, offset

    def __enter__(self):
        assert not self.closed
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.close()
