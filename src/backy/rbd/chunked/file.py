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
    stats: dict
    closed: bool
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
        stats: Optional[dict] = None,
    ):
        self.name = str(name)
        self.store = store
        self.stats = stats if stats is not None else dict()
        self.closed = False
        # This indicates that writes should be temporary and no modify
        # the metadata when closing.
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
            raise ValueError("`whence` does not support mode {}".format(whence))

        if position < 0:
            raise ValueError("Can not seek before the beginning of a file.")
        if position > self.size:
            self.truncate(position)

        self._position = position
        return position

    def truncate(self, target: Optional[int] = None, /) -> None:
        assert "w" in self.mode and not self.closed
        if target is None:
            target = self._position
        # Remove chunks past the size
        to_remove = set(
            key
            for key in set(self._mapping) | set(self._chunks)
            if key * Chunk.CHUNK_SIZE >= target
        )
        for key in to_remove:
            self._mapping.pop(key, None)
            self._chunks.pop(key, None)

        # Fill up the missing parts with zeroes.
        orig_pos = self._position
        self._position = self.size

        if target > self._position:
            # fill first chunk
            data = min(target - self._position, Chunk.CHUNK_SIZE) * b"\00"
            chunk, _, offset = self._current_chunk()
            written, _ = chunk.write(offset, data)
            self._position += written

        if target > self._position:
            chunk, chunk_id, offset = self._current_chunk()
            assert offset == 0
            written, _ = chunk.write(offset, Chunk.CHUNK_SIZE * b"\00")
            assert written == Chunk.CHUNK_SIZE
            self._position += Chunk.CHUNK_SIZE
            empty_chunk_hash = chunk.flush()
            assert empty_chunk_hash
            self._mapping[chunk_id] = empty_chunk_hash

            while target > self._position:
                chunk_id = self._position // Chunk.CHUNK_SIZE
                self._mapping[chunk_id] = empty_chunk_hash
                self._position += Chunk.CHUNK_SIZE

        # Filling up should have caused our position to have moved properly.
        # Note that we will always fill full chunks. This is safe as data after
        # the size limit can not be revealed.
        assert self._position >= target
        self._position = orig_pos
        self.size = target

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
        self.stats.setdefault("bytes_written", 0)
        self.stats["bytes_written"] += len(data)
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
                self.store,
                self._mapping.get(chunk_id),
                self.stats.setdefault("chunk_stats", dict()),
            )
        count = self._access_stats[chunk_id][0]
        self._access_stats[chunk_id] = (count + 1, time.time())
        return self._chunks[chunk_id], chunk_id, offset

    def __enter__(self):
        assert not self.closed
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.close()
