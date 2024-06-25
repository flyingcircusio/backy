import os.path
import subprocess
from unittest import mock

from backy.rbd.chunked import ChunkedFileBackend
from backy.rbd.conftest import create_rev
from backy.rbd.sources.file import File
from backy.utils import CHUNK_SIZE


def test_config(rbdbackup, tmp_path):
    assert rbdbackup.path == tmp_path
    assert isinstance(rbdbackup.source, File)
    assert rbdbackup.source.filename == "input-file"


def test_restore_target(rbdbackup, log):
    source = "input-file"
    target = "restore.img"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    r = create_rev(rbdbackup, {"daily"})
    rbdbackup.backup(r.uuid)
    rbdbackup.restore(r.uuid, target)
    with open(source, "rb") as s, open(target, "rb") as t:
        assert s.read() == t.read()


def test_restore_stdout(rbdbackup, capfd, log):
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    r = create_rev(rbdbackup, {"daily"})
    rbdbackup.backup(r.uuid)
    rbdbackup.restore(r.uuid, "-")
    assert not os.path.exists("-")
    out, err = capfd.readouterr()
    assert "volume contents\n" == out


def test_restore_backy_extract(rbdbackup, monkeypatch, log):
    check_output = mock.Mock(return_value="backy-extract 1.1.0")
    monkeypatch.setattr(subprocess, "check_output", check_output)
    rbdbackup.restore_backy_extract = mock.Mock()
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"a" * CHUNK_SIZE)
    r = create_rev(rbdbackup, {"daily"})
    rbdbackup.backup(r.uuid)
    rbdbackup.restore(r.uuid, "restore.img")
    check_output.assert_called()
    rbdbackup.restore_backy_extract.assert_called_once_with(
        rbdbackup.find("0"), "restore.img"
    )


def test_backup_corrupted(rbdbackup, log):
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    r = create_rev(rbdbackup, {"daily"})
    rbdbackup.backup(r.uuid)

    store = ChunkedFileBackend(rbdbackup.history[0], log).store
    chunk_path = store.chunk_path(next(iter(store.seen)))
    os.chmod(chunk_path, 0o664)
    with open(chunk_path, "wb") as f:
        f.write(b"invalid")
    r2 = create_rev(rbdbackup, {"daily"})
    rbdbackup.backup(r2.uuid)

    assert rbdbackup.history == []
    assert not os.path.exists(chunk_path)
