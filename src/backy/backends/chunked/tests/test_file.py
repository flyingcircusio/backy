from backy.backends.chunked.chunk import Chunk
from backy.backends.chunked.file import File
from backy.backends.chunked.store import Store
import io
import lzo
import os
import pytest
import random


def test_simple_open_write_read_seek(tmpdir):
    store = Store(str(tmpdir))

    f = File(str(tmpdir / 'asdf'), store)
    f.write(b'asdf')
    f.seek(0)
    assert f.read() == b'asdf'
    f.close()

    f = File(str(tmpdir / 'asdf'), store)
    assert f.read() == b'asdf'
    f.seek(0)
    assert f.read(0) == b''
    assert f.tell() == 0
    assert f.read(1) == b'a'
    assert f.tell() == 1
    assert f.read(2) == b'sd'
    assert f.tell() == 3
    assert f.read(5) == b'f'
    assert f.tell() == 4
    f.close()


def test_file_api(tmpdir):
    store = Store(str(tmpdir))
    f = File(str(tmpdir / 'asdf'), store)
    assert not f.isatty()
    assert f.readable()
    assert f.writable()
    f.close()
    assert not f.readable()
    assert not f.writable()
    f = File(str(tmpdir / 'asdf'), store, mode='w')
    assert not f.readable()
    assert f.writable()
    assert f.seekable()
    f.close()
    f = File(str(tmpdir / 'asdf'), store, mode='r',)
    assert f.readable()
    assert not f.writable()


def test_underrun_gets_noticed(tmpdir):
    # This is a somewhat weird case. this means we're referring to a block
    # that is correctly compressed but has too little data in it.
    store = Store(str(tmpdir))
    with File(str(tmpdir / 'asdf'), store) as f:
        f.write(b'asdfasdfasdf')
        f._flush_chunks(0)
        hash = list(f._mapping.values())[0]
        hash_file = store.chunk_path(hash)
        os.chmod(hash_file, 0o660)
        with open(hash_file, 'wb') as cf:
            cf.write(lzo.compress(b'asdf'))

        f.seek(0)
        with pytest.raises(ValueError):
            f.read()


def test_file_seek(tmpdir):
    store = Store(str(tmpdir))
    f = File(str(tmpdir / 'asdf'), store)
    with pytest.raises(ValueError):
        f.seek(-1)

    f.seek(0)
    f.write(b'asdf')
    f.seek(0, io.SEEK_END)
    assert f.tell() == 4
    assert f.read() == b''
    f.seek(-2, io.SEEK_CUR)
    assert f.tell() == 2
    assert f.read() == b'df'
    f.seek(-2, io.SEEK_CUR)
    f.seek(1, io.SEEK_CUR)
    assert f.read() == b'f'
    with pytest.raises(ValueError):
        f.seek(1, 'asdf')

    # These are interesting cases. Seek to "nowhere" after the end of the
    # file which means those blocks should be zeroes. Even skip multiple
    # blocks and start writing in the middle.
    f.seek(100)
    f.write(b'bsdf')
    f.seek(0)
    x = f.read()
    assert x.startswith(b'asdf\x00\x00\x00')
    assert x.endswith(b'\x00\x00\x00\x00bsdf')
    assert len(x) == 104
    assert x[4:-4] == 96 * b'\x00'
    f.seek(20 * 1024 * 1024 + 100)
    f.write(b'csdf')
    f.seek(0)
    x = f.read()
    assert len(x) == 20 * 1024 * 1024 + 100 + 4
    assert x[20 * 1024 * 1024 - 100:20 * 1024 * 1024 + 100] == 200 * b'\x00'
    assert x[20 * 1024 * 1024 + 98:20 * 1024 * 1024 + 104] == b'\x00\x00csdf'

    f.seek(10)
    f.truncate()
    f.seek(0)
    assert f.read() == b'asdf\x00\x00\x00\x00\x00\x00'
    f.seek(0, io.SEEK_END)
    assert f.tell() == 10
    f.close()
    f = File(str(tmpdir / 'asdf'), store, mode='r')
    f.read() == b'asdf\x00\x00\x00\x00\x00\x00'

    f.close()


def test_simple_open_nonexisting(tmpdir):
    store = Store(str(tmpdir))
    with pytest.raises(FileNotFoundError):
        File(str(tmpdir / 'asdf'), store, mode='r')


def test_continuously_updated_file(tmpdir):
    store = Store(str(tmpdir))
    sample = open(str(tmpdir / 'bsdf'), 'wb')
    f = File(str(tmpdir / 'asdf'), store)
    for i in range(20):
        chunk = ((random.randint(0, 255),) *
                 random.randint(1, 5 * Chunk.CHUNK_SIZE))
        chunk = bytes(chunk)
        sample.write(chunk)
        f.write(chunk)
        assert sample.tell() == f.tell()

    f.close()
    sample.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    f = File(str(tmpdir / 'asdf'), store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data

    store.validate_chunks()

    store.users.append(f)
    store.purge()

    sample.close()
    f.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    with File(str(tmpdir / 'asdf'), store) as f:
        data = sample.read()
        chunked_data = f.read()
        assert data == chunked_data


def test_seeky_updated_file(tmpdir):
    store = Store(str(tmpdir))

    sample = open(str(tmpdir / 'bsdf'), 'wb')
    f = File(str(tmpdir / 'asdf'), store)
    for i in range(20):
        chunk = ((random.randint(0, 255),) *
                 random.randint(1, 5 * Chunk.CHUNK_SIZE))
        chunk = bytes(chunk)
        offset = random.randint(0, f.size)
        sample.seek(offset)
        sample.write(chunk)
        f.seek(offset)
        f.write(chunk)
        assert sample.tell() == f.tell()

    f.close()
    sample.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    f = File(str(tmpdir / 'asdf'), store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data

    store.validate_chunks()

    store.users.append(f)
    store.purge()

    sample.close()
    f.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    with File(str(tmpdir / 'asdf'), store) as f:
        data = sample.read()
        chunked_data = f.read()
        assert data == chunked_data


def test_truncate(tmpdir):
    store = Store(str(tmpdir))

    f = File(str(tmpdir / 'asdf'), store)
    for i in range(5):
        # Write 20 chunks
        chunk = b' ' * Chunk.CHUNK_SIZE
        f.write(chunk)

    f.flush()

    space_hash = (
        'c01b5d75bfe6a1fa5bca6e492c5ab09a')
    assert f._mapping == {i: space_hash for i in range(5)}

    f.truncate(50)
    f.seek(0)
    assert f.read() == b' ' * 50
    # Interesting optimization: truncating re-uses the existing
    # chunk and only limits how much we read from it.
    assert f._mapping == {0: space_hash}
    f.close()


def test_rplus_and_append_positions(tmpdir):
    store = Store(str(tmpdir))

    with File(str(tmpdir / 'asdf'), store) as f:
        f.write(b'asdf')

    with File(str(tmpdir / 'asdf'), store, mode='r+') as f:
        assert f.tell() == 0
        f.write(b'bsdf')

    with File(str(tmpdir / 'asdf'), store, mode='a') as f:
        assert f.tell() == 4
        f.write(b'csdf')

    with File(str(tmpdir / 'asdf'), store) as f:
        assert f.read() == b'bsdfcsdf'
