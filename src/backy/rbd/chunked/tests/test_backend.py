from backy.rbd.chunked import ChunkedFileBackend
from backy.revision import Revision


def test_overlay(rbdbackup, log):
    r = Revision.create(rbdbackup, set(), log)
    backend = ChunkedFileBackend(r, log)
    # Write 1 version to the file
    f = backend.open("w")
    f.write(b"asdf")
    f.close()
    with backend.open("r") as f:
        assert f.read() == b"asdf"
    # Open the file in overlay, write to it
    f = backend.open("o")
    assert f.read() == b"asdf"
    f.seek(0)
    f.write(b"bsdf")
    f.seek(0)
    assert f.read() == b"bsdf"
    f.close()
    # Close the file and open it again results in the original content
    f = backend.open("r")
    assert f.read() == b"asdf"
    f.close()


def test_purge(rbdbackup, log):
    r = Revision.create(rbdbackup, set(), log)
    backend = ChunkedFileBackend(r, log)
    # Write 1 version to the file
    f = backend.open("w")
    f.write(b"asdf")
    f.close()
    r.materialize()
    remote = Revision(rbdbackup, log)  # remote revision without local data
    remote.server = "remote"
    remote.materialize()
    rbdbackup.scan()
    # Reassign as the scan will create a new reference
    r = rbdbackup.history[0]
    assert len(list(r.backend.store.ls())) == 1
    r.backend.purge()
    assert len(list(r.backend.store.ls())) == 1
    r.remove()
    r.backend.purge()
    assert len(list(r.backend.store.ls())) == 0
