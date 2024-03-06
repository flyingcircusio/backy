import io
import random

import lzo
import pytest

from backy.backends.chunked.chunk import Chunk, InconsistentHash
from backy.backends.chunked.file import File
from backy.backends.chunked.store import Store


def test_simple_open_write_read_seek(tmp_path, log):
    store = Store(tmp_path, log)

    f = File(tmp_path / "asdf", store)
    f.write(b"asdf")
    f.seek(0)
    assert f.read() == b"asdf"
    f.close()

    f = File(tmp_path / "asdf", store)
    assert f.read() == b"asdf"
    f.seek(0)
    assert f.read(0) == b""
    assert f.tell() == 0
    assert f.read(1) == b"a"
    assert f.tell() == 1
    assert f.read(2) == b"sd"
    assert f.tell() == 3
    assert f.read(5) == b"f"
    assert f.tell() == 4
    f.close()


def test_file_api(tmp_path, log):
    store = Store(tmp_path, log)
    f = File(tmp_path / "asdf", store)
    assert not f.isatty()
    assert f.readable()
    assert f.writable()
    f.close()
    assert not f.readable()
    assert not f.writable()
    f = File(tmp_path / "asdf", store, mode="w")
    assert not f.readable()
    assert f.writable()
    assert f.seekable()
    f.close()
    f = File(tmp_path / "asdf", store, mode="r")
    assert f.readable()
    assert not f.writable()


def test_underrun_gets_noticed(tmp_path, log):
    # This is a somewhat weird case. this means we're referring to a block
    # that is correctly compressed but has too little data in it.
    store = Store(tmp_path, log)
    with File(tmp_path / "asdf", store) as f:
        f.write(b"asdfasdfasdf")
        f._flush_chunks(0)
        hash = list(f._mapping.values())[0]
        hash_file = store.chunk_path(hash)
        hash_file.chmod(0o660)
        with open(hash_file, "wb") as cf:
            cf.write(lzo.compress(b"asdf"))

        f.seek(0)
        with pytest.raises(InconsistentHash):
            f.read()


def test_file_seek(tmp_path, log):
    store = Store(tmp_path, log)
    f = File(tmp_path / "asdf", store)
    with pytest.raises(ValueError):
        f.seek(-1)

    f.seek(0)
    f.write(b"asdf")
    f.seek(0, io.SEEK_END)
    assert f.tell() == 4
    assert f.read() == b""
    f.seek(-2, io.SEEK_CUR)
    assert f.tell() == 2
    assert f.read() == b"df"
    f.seek(-2, io.SEEK_CUR)
    f.seek(1, io.SEEK_CUR)
    assert f.read() == b"f"
    with pytest.raises(ValueError):
        f.seek(1, "asdf")

    # These are interesting cases. Seek to "nowhere" after the end of the
    # file which means those blocks should be zeroes. Even skip multiple
    # blocks and start writing in the middle.
    f.seek(100)
    f.write(b"bsdf")
    f.seek(0)
    x = f.read()
    assert x.startswith(b"asdf\x00\x00\x00")
    assert x.endswith(b"\x00\x00\x00\x00bsdf")
    assert len(x) == 104
    assert x[4:-4] == 96 * b"\x00"
    f.seek(20 * 1024 * 1024 + 100)
    f.write(b"csdf")
    f.seek(0)
    x = f.read()
    assert len(x) == 20 * 1024 * 1024 + 100 + 4
    assert x[20 * 1024 * 1024 - 100 : 20 * 1024 * 1024 + 100] == 200 * b"\x00"
    assert x[20 * 1024 * 1024 + 98 : 20 * 1024 * 1024 + 104] == b"\x00\x00csdf"

    f.seek(10)
    f.truncate()
    f.seek(0)
    assert f.read() == b"asdf\x00\x00\x00\x00\x00\x00"
    f.seek(0, io.SEEK_END)
    assert f.tell() == 10
    f.close()
    f = File(tmp_path / "asdf", store, mode="r")
    assert f.read() == b"asdf\x00\x00\x00\x00\x00\x00"

    f.close()


def test_simple_open_nonexisting(tmp_path, log):
    store = Store(tmp_path, log)
    with pytest.raises(FileNotFoundError):
        File(tmp_path / "asdf", store, mode="r")


def test_continuously_updated_file(tmp_path, log):
    store = Store(tmp_path, log)
    sample = open(tmp_path / "bsdf", "wb")
    f = File(tmp_path / "asdf", store)
    for i in range(20):
        chunk = (random.randint(0, 255),) * random.randint(
            1, 5 * Chunk.CHUNK_SIZE
        )
        chunk = bytes(chunk)
        sample.write(chunk)
        f.write(chunk)
        assert sample.tell() == f.tell()

    f.close()
    sample.close()

    sample = open(tmp_path / "bsdf", "rb")
    f = File(tmp_path / "asdf", store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data

    store.validate_chunks()

    store.purge(set(f._mapping.values()))

    sample.close()
    f.close()

    sample = open(tmp_path / "bsdf", "rb")
    with File(tmp_path / "asdf", store) as f:
        data = sample.read()
        chunked_data = f.read()
        assert data == chunked_data


def test_seeky_updated_file(tmp_path, log):
    store = Store(tmp_path, log)

    sample = open(tmp_path / "bsdf", "wb")
    f = File(tmp_path / "asdf", store)
    for i in range(20):
        chunk = (random.randint(0, 255),) * random.randint(
            1, 5 * Chunk.CHUNK_SIZE
        )
        chunk = bytes(chunk)
        offset = random.randint(0, f.size)
        sample.seek(offset)
        sample.write(chunk)
        f.seek(offset)
        f.write(chunk)
        assert sample.tell() == f.tell()

    f.close()
    sample.close()

    sample = open(tmp_path / "bsdf", "rb")
    f = File(tmp_path / "asdf", store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data

    store.validate_chunks()

    store.purge(set(f._mapping.values()))

    sample.close()
    f.close()

    sample = open(tmp_path / "bsdf", "rb")
    with File(tmp_path / "asdf", store) as f:
        data = sample.read()
        chunked_data = f.read()
        assert data == chunked_data


def test_truncate(tmp_path, log):
    store = Store(tmp_path, log)

    f = File(tmp_path / "asdf", store)
    for i in range(5):
        # Write 20 chunks
        chunk = b" " * Chunk.CHUNK_SIZE
        f.write(chunk)

    f.flush()

    space_hash = "c01b5d75bfe6a1fa5bca6e492c5ab09a"
    assert f._mapping == {i: space_hash for i in range(5)}

    f.truncate(50)
    f.seek(0)
    assert f.read() == b" " * 50
    # Interesting optimization: truncating re-uses the existing
    # chunk and only limits how much we read from it.
    assert f._mapping == {0: space_hash}
    f.close()


def test_rplus_and_append_positions(tmp_path, log):
    store = Store(tmp_path, log)

    with File(tmp_path / "asdf", store) as f:
        f.write(b"asdf")

    with File(tmp_path / "asdf", store, mode="r+") as f:
        assert f.tell() == 0
        f.write(b"bsdf")

    with File(tmp_path / "asdf", store, mode="a") as f:
        assert f.tell() == 4
        f.write(b"csdf")

    with File(tmp_path / "asdf", store) as f:
        assert f.read() == b"bsdfcsdf"
