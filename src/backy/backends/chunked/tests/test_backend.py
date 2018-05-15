from backy.backends.chunked import ChunkedFileBackend
from backy.revision import Revision
import pytest
import os


def test_overlay(simple_file_config):
    r = Revision(simple_file_config)
    assert isinstance(r.backend, ChunkedFileBackend)
    # Write 1 version to the file
    f = r.open('w')
    f.write(b'asdf')
    f.close()
    with r.open('r') as f:
        assert f.read() == b'asdf'
    # Open the file in overlay, write to it
    f = r.open('o')
    assert f.read() == b'asdf'
    f.seek(0)
    f.write(b'bsdf')
    f.seek(0)
    assert f.read() == b'bsdf'
    f.close()
    # Close the file and open it again results in the original content
    f = r.open('r')
    assert f.read() == b'asdf'
    f.close()


def test_purge(simple_file_config):
    b = simple_file_config
    r = Revision(b)
    # Write 1 version to the file
    f = r.open('w')
    f.write(b'asdf')
    f.close()
    r.materialize()
    b.scan()
    # Reassign as the scan will create a new reference
    r = b.history[0]
    assert len(list(r.backend.store.ls())) == 1
    r.backend.purge()
    assert len(list(r.backend.store.ls())) == 1
    r.remove()
    r.backend.purge()
    assert len(list(r.backend.store.ls())) == 0


def test_scrub_light(simple_file_config):
    b = simple_file_config
    r = Revision(b)
    r.materialize()
    b.scan()
    f = r.open('w')
    f.write(b'asdf')
    f.close()
    assert r.backend.scrub(b, 'light') == 0
    for x, _, _ in r.backend.store.ls():
        os.unlink(x)
    assert r.backend.scrub(b, 'light') == 1


def test_scrub_deep(simple_file_config):
    b = simple_file_config
    r = Revision(b)
    r.materialize()
    b.scan()
    f = r.open('w')
    f.write(b'asdf')
    f.close()
    assert r.backend.scrub(b, 'deep') == 0
    for x, _, _ in r.backend.store.ls():
        os.chmod(x, 0o660)
        with open(x, 'w') as f:
            f.write('foobar')
    assert r.backend.scrub(b, 'deep') == 1


def test_scrub_wrong_type(simple_file_config):
    b = simple_file_config
    r = Revision(b)
    r.materialize()
    b.scan()
    with pytest.raises(RuntimeError):
        r.backend.scrub(b, 'asdf')
