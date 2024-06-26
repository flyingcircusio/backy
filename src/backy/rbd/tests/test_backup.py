import os.path
import subprocess
from unittest import mock

from backy.rbd.chunked import ChunkedFileBackend
from backy.rbd.conftest import create_rev
from backy.rbd.sources.file import File
from backy.utils import CHUNK_SIZE


def test_config(rbdrepository, tmp_path):
    assert rbdrepository.path == tmp_path
    assert isinstance(rbdrepository.source, File)
    assert rbdrepository.source.filename == "input-file"


def test_restore_target(rbdrepository, log):
    source = "input-file"
    target = "restore.img"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    r = create_rev(rbdrepository, {"daily"})
    rbdrepository.backup(r.uuid)
    rbdrepository.restore(r.uuid, target)
    with open(source, "rb") as s, open(target, "rb") as t:
        assert s.read() == t.read()


def test_restore_stdout(rbdrepository, capfd, log):
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    r = create_rev(rbdrepository, {"daily"})
    rbdrepository.backup(r.uuid)
    rbdrepository.restore(r.uuid, "-")
    assert not os.path.exists("-")
    out, err = capfd.readouterr()
    assert "volume contents\n" == out


def test_restore_backy_extract(rbdrepository, monkeypatch, log):
    check_output = mock.Mock(return_value="backy-extract 1.1.0")
    monkeypatch.setattr(subprocess, "check_output", check_output)
    rbdrepository.restore_backy_extract = mock.Mock()
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"a" * CHUNK_SIZE)
    r = create_rev(rbdrepository, {"daily"})
    rbdrepository.backup(r.uuid)
    rbdrepository.restore(r.uuid, "restore.img")
    check_output.assert_called()
    rbdrepository.restore_backy_extract.assert_called_once_with(
        rbdrepository.find("0"), "restore.img"
    )


def test_backup_corrupted(rbdrepository, log):
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    r = create_rev(rbdrepository, {"daily"})
    rbdrepository.backup(r.uuid)

    store = ChunkedFileBackend(rbdrepository.history[0], log).store
    chunk_path = store.chunk_path(next(iter(store.seen)))
    os.chmod(chunk_path, 0o664)
    with open(chunk_path, "wb") as f:
        f.write(b"invalid")
    r2 = create_rev(rbdrepository, {"daily"})
    rbdrepository.backup(r2.uuid)

    assert rbdrepository.history == []
    assert not os.path.exists(chunk_path)
