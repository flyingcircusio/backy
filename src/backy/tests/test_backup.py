import os.path
import subprocess
from unittest import mock

import pytest

import backy.utils
from backy.revision import Revision
from backy.sources.file import File
from backy.utils import CHUNK_SIZE


def test_config(simple_file_config, tmp_path):
    backup = simple_file_config

    assert backup.path == tmp_path
    assert isinstance(backup.source, File)
    assert backup.source.filename == "input-file"


def test_find(simple_file_config, tmp_path, log):
    backup = simple_file_config
    rev = Revision.create(backup, set(), log, uuid="123-456")
    rev.materialize()
    backup.scan()
    assert tmp_path / "123-456" == backup.find("0").filename


def test_find_should_raise_if_not_found(simple_file_config, log):
    backup = simple_file_config
    rev = Revision.create(backup, set(), log)
    rev.materialize()
    backup.scan()
    with pytest.raises(KeyError):
        backup.find("no such revision")


def test_restore_target(simple_file_config):
    backup = simple_file_config
    source = "input-file"
    target = "restore.img"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    backup.backup({"daily"})
    backup.restore("0", target)
    with open(source, "rb") as s, open(target, "rb") as t:
        assert s.read() == t.read()


def test_restore_stdout(simple_file_config, capfd):
    backup = simple_file_config
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    backup.backup({"daily"})
    backup.restore("0", "-")
    assert not os.path.exists("-")
    out, err = capfd.readouterr()
    assert "volume contents\n" == out


def test_restore_backy_extract(simple_file_config, monkeypatch):
    check_output = mock.Mock(return_value="backy-extract 1.1.0")
    monkeypatch.setattr(subprocess, "check_output", check_output)
    backup = simple_file_config
    backup.restore_backy_extract = mock.Mock()
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"a" * CHUNK_SIZE)
    backup.backup({"daily"})
    backup.restore("0", "restore.img")
    check_output.assert_called()
    backup.restore_backy_extract.assert_called_once_with(
        backup.find("0"), "restore.img"
    )


def test_backup_corrupted(simple_file_config):
    backup = simple_file_config
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    backup.backup({"daily"})

    store = backup.history[0].backend.store
    chunk_path = store.chunk_path(next(iter(store.seen)))
    os.chmod(chunk_path, 0o664)
    with open(chunk_path, "wb") as f:
        f.write(b"invalid")
    backup.backup({"daily"})

    assert backup.history == []
    assert not os.path.exists(chunk_path)


def test_restore_mixed_backend(simple_file_config):
    backup = simple_file_config
    backup.default_backend_type = "cowfile"
    source = "input-file"
    out = "output-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    backup.backup({"daily"})

    with open(source, "wb") as f:
        f.write(b"meow\n")
    backup.default_backend_type = "chunked"
    backup.backup({"daily"})

    assert len(backup.history) == 2

    backup.restore("1", out)
    with open(out, "rb") as f:
        assert f.read() == b"volume contents\n"

    backup.restore("0", out)
    with open(out, "rb") as f:
        assert f.read() == b"meow\n"
