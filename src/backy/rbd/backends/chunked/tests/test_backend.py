import os

import pytest

from backy.backends.chunked import ChunkedFileBackend
from backy.revision import Revision


def test_overlay(simple_file_config, log):
    r = Revision.create(simple_file_config, set(), log)
    assert isinstance(r.backend, ChunkedFileBackend)
    # Write 1 version to the file
    f = r.backend.open("w")
    f.write(b"asdf")
    f.close()
    with r.backend.open("r") as f:
        assert f.read() == b"asdf"
    # Open the file in overlay, write to it
    f = r.backend.open("o")
    assert f.read() == b"asdf"
    f.seek(0)
    f.write(b"bsdf")
    f.seek(0)
    assert f.read() == b"bsdf"
    f.close()
    # Close the file and open it again results in the original content
    f = r.backend.open("r")
    assert f.read() == b"asdf"
    f.close()


def test_purge(simple_file_config, log):
    b = simple_file_config
    r = Revision.create(b, set(), log)
    # Write 1 version to the file
    f = r.backend.open("w")
    f.write(b"asdf")
    f.close()
    r.materialize()
    remote = Revision(b, log)  # remote revision without local data
    remote.server = "remote"
    remote.materialize()
    b.scan()
    # Reassign as the scan will create a new reference
    r = b.history[0]
    assert len(list(r.backend.store.ls())) == 1
    r.backend.purge()
    assert len(list(r.backend.store.ls())) == 1
    r.remove()
    r.backend.purge()
    assert len(list(r.backend.store.ls())) == 0
