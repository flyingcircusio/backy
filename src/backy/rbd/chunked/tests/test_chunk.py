from unittest.mock import Mock

import lzo
import pytest

from backy.rbd.chunked.chunk import Chunk, InconsistentHash, hash
from backy.rbd.chunked.file import File
from backy.rbd.chunked.store import Store

SPACE_CHUNK = b" " * Chunk.CHUNK_SIZE
SPACE_CHUNK_HASH = "c01b5d75bfe6a1fa5bca6e492c5ab09a"


def test_chunk_read_write_update(tmp_path, log):
    store = Store(tmp_path / "store", log)
    f = File(tmp_path / "asdf", store)

    chunk = Chunk(store, None)
    chunk.write(0, b"asdf")
    chunk.write(4, b"bsdf")
    assert chunk.read(0) == (b"asdfbsdf", -1)
    chunk.write(1, b"xxxx")
    assert chunk.read(0) == (b"axxxxsdf", -1)


def test_chunk_write_partial_offset(tmp_path, log):
    store = Store(tmp_path / "store", log)
    f = File(tmp_path / "asdf", store)

    chunk = Chunk(store, None)
    # Write data that fits exactly into this chunk. Nothing remains
    # to be written.
    result = chunk.write(0, SPACE_CHUNK)
    assert result == (Chunk.CHUNK_SIZE, b"")
    # Write data that doesn't fit exactly into this chunk. This means
    # we have remaining data that needs to go into another chunk.
    result = chunk.write(10, SPACE_CHUNK)
    assert result == (Chunk.CHUNK_SIZE - 10, b" " * 10)

    chunk.flush()
    assert chunk.hash == SPACE_CHUNK_HASH
    store_state = store.chunk_path(SPACE_CHUNK_HASH).stat()

    with open(store.chunk_path(chunk.hash), "rb") as store_file:
        data = store_file.read()
        data = lzo.decompress(data)
        assert data == SPACE_CHUNK

    # Check that we can edit and flush again. Check that the store file
    # wasn't touched.
    chunk.write(0, b"      ")
    chunk.flush()
    assert store_state == store.chunk_path(SPACE_CHUNK_HASH).stat()


def test_chunk_read_existing(tmp_path, log):
    store = Store(tmp_path / "store", log)

    chunk_hash = hash("asdf")
    p = store.chunk_path(chunk_hash)
    with open(p, "wb") as existing:
        existing.write(lzo.compress(b"asdf"))

    f = File(tmp_path / "asdf", store)

    chunk = Chunk(store, chunk_hash)
    assert chunk.read(0) == (b"asdf", -1)
    assert chunk.read(0, 10) == (b"asdf", 6)

    # Check that flushing a file that hasn't been written to does not fail.
    chunk.flush()


def test_chunk_write_existing_partial_joins_with_existing_data(tmp_path, log):
    store = Store(tmp_path / "store", log)

    chunk_hash = hash("asdf")
    p = store.chunk_path(chunk_hash)
    with open(p, "wb") as existing:
        existing.write(lzo.compress(b"asdf"))

    f = File(tmp_path / "asdf", store)

    chunk = Chunk(store, chunk_hash)
    chunk._read_existing = Mock(side_effect=chunk._read_existing)
    chunk.write(2, b"xxsdf")
    assert chunk.read(0) == (b"asxxsdf", -1)
    chunk._read_existing.assert_called()


def test_chunk_fails_wrong_content(tmp_path, log):
    store = Store(tmp_path / "store", log)

    chunk_hash = hash("asdf")
    p = store.chunk_path(chunk_hash)
    with open(p, "wb") as existing:
        existing.write(lzo.compress(b"bsdf"))

    chunk = Chunk(store, chunk_hash)
    with pytest.raises(InconsistentHash):
        chunk.read(0)


def test_chunk_write_existing_partial_complete_does_not_read_existing_data(
    tmp_path, log
):
    store = Store(tmp_path / "store", log)

    p = store.chunk_path("asdf")
    p.parent.mkdir(parents=True)
    with open(p, "wb") as existing:
        existing.write(lzo.compress(b"asdf"))

    f = File(tmp_path / "asdf", store)

    chunk = Chunk(store, "asdf")
    chunk._read_existing = Mock(side_effect=chunk._read_existing)
    chunk.write(0, b"X" * Chunk.CHUNK_SIZE)
    chunk._read_existing.assert_not_called()
    assert chunk.read(0, 3) == (b"XXX", 0)
