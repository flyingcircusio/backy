from backy.backends.chunked import ChunkedFile, ChunkStore
import random


def test_simple_open_write_read_seek(tmpdir):
    store = ChunkStore(str(tmpdir))

    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    f.write(b'asdf')
    f.seek(0)
    assert f.read() == b'asdf'
    f.close()

    f = ChunkedFile(str(tmpdir / 'asdf'), store)
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
    store = ChunkStore(str(tmpdir))

    sample = open(str(tmpdir / 'bsdf'), 'wb')
    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    for i in range(20):
        chunk = ((random.randint(0, 255),) *
                 random.randint(1, 5 * ChunkedFile.CHUNK_SIZE))
        chunk = bytes(chunk)
        sample.write(chunk)
        f.write(chunk)
        assert sample.tell() == f.tell()

    f.close()
    sample.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data

    store.validate()

    store.users.append(f)
    store.expunge()

    sample.close()
    f.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data


def test_seeky_updated_file(tmpdir):
    store = ChunkStore(str(tmpdir))

    sample = open(str(tmpdir / 'bsdf'), 'wb')
    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    for i in range(20):
        chunk = ((random.randint(0, 255),) *
                 random.randint(1, 5 * ChunkedFile.CHUNK_SIZE))
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
    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data

    store.validate()

    store.users.append(f)
    store.expunge()

    sample.close()
    f.close()

    sample = open(str(tmpdir / 'bsdf'), 'rb')
    f = ChunkedFile(str(tmpdir / 'asdf'), store)
    data = sample.read()
    chunked_data = f.read()
    assert data == chunked_data
