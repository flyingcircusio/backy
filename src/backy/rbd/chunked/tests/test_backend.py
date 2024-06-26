from backy.rbd.chunked import ChunkedFileBackend
from backy.revision import Revision


def test_overlay(rbdrepository, log):
    r = Revision.create(rbdrepository, set(), log)
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


def test_purge(rbdrepository, log):
    r = Revision.create(rbdrepository, set(), log)
    backend = ChunkedFileBackend(r, log)
    # Write 1 version to the file
    f = backend.open("w")
    f.write(b"asdf")
    f.close()
    r.materialize()
    remote = Revision(rbdrepository, log)  # remote revision without local data
    remote.server = "remote"
    remote.materialize()
    rbdrepository.scan()
    # Reassign as the scan will create a new reference
    r = rbdrepository.history[0]
    assert len(list(backend.store.ls())) == 1
    backend.purge()
    assert len(list(backend.store.ls())) == 1
    r.remove()
    backend.purge()
    assert len(list(backend.store.ls())) == 0
