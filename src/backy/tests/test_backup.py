import os.path

import pytest
import yaml

import backy.utils
from backy.backup import Backup
from backy.revision import Revision
from backy.sources.file import File


def test_config(simple_file_config, tmpdir):
    backup = simple_file_config

    assert backup.path == str(tmpdir)
    assert isinstance(backup.source, File)
    assert backup.source.filename == "input-file"


def test_find(simple_file_config, tmpdir):
    backup = simple_file_config
    rev = Revision(backup, "123-456", backup)
    rev.timestamp = backy.utils.now()
    rev.materialize()
    backup.scan()
    assert str(tmpdir / "123-456") == backup.find(0).filename


def test_find_should_raise_if_not_found(simple_file_config):
    backup = simple_file_config
    rev = Revision(backup, "123-456")
    rev.timestamp = backy.utils.now()
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
    backup.restore(0, target)
    with open(source, "rb") as s, open(target, "rb") as t:
        assert s.read() == t.read()


def test_restore_stdout(simple_file_config, capfd):
    backup = simple_file_config
    source = "input-file"
    with open(source, "wb") as f:
        f.write(b"volume contents\n")
    backup.backup({"daily"})
    backup.restore(0, "-")
    assert not os.path.exists("-")
    out, err = capfd.readouterr()
    assert "volume contents\n" == out
