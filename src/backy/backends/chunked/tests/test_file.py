from backy.backends.chunked.file import File
from backy.backends.chunked.store import Store
from backy.backends.chunked.chunk import Chunk
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
    f = File(str(tmpdir / 'asdf'), store)
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
    f = File(str(tmpdir / 'asdf'), store)
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
