from backy.backup import Backup
from backy.sources.file import File
import json
import os.path
import pytest
import shutil


fixtures = os.path.dirname(__file__) + '/samples'


@pytest.fixture
def simple_file_config(tmpdir):
    shutil.copy(fixtures + '/simple_file/config', str(tmpdir))
    return Backup(str(tmpdir))


def test_config(simple_file_config, tmpdir):
    backup = simple_file_config

    assert backup.path == str(tmpdir)
    assert isinstance(backup.source, File)
    assert backup.source.filename == "input-file"


def test_empty_revisions(simple_file_config):
    backup = simple_file_config

    backup._scan_revisions()
    assert backup.revision_history == []


def test_load_revisions(simple_file_config, tmpdir):
    backup = simple_file_config

    with open(str(tmpdir / '123-123.rev'), 'wb') as f:
        f.write(b'{"uuid": "123-123", "timestamp": 1, "parent": null}')
    with open(str(tmpdir / '123-124.rev'), 'wb') as f:
        f.write(b'{"uuid": "124-124", "timestamp": 2, "parent": "123-123"}')

    backup._scan_revisions()
    assert [x.uuid for x in backup.revision_history] == ["123-123", "124-124"]

    assert backup.revision_history[1].uuid == "124-124"
    assert backup.revision_history[1].parent == "123-123"

    assert backup.find_revisions("all") == backup.revision_history

    assert backup.find_revision("last").uuid == "124-124"
    assert backup.find_revision(0).uuid == "124-124"
    assert backup.find_revision(1).uuid == "123-123"
    assert backup.find_revision("124-124").uuid == "124-124"

    with pytest.raises(KeyError):
        backup.find_revision("125-125")

    assert backup.find_revisions(1) == [backup.find_revision(1)]


def test_find_revision_empty(simple_file_config):
    backup = simple_file_config

    backup._scan_revisions()

    with pytest.raises(KeyError):
        backup.find_revision(-1)

    with pytest.raises(KeyError):
        backup.find_revision("last")

    with pytest.raises(KeyError):
        backup.find_revision("fdasfdka")


def test_init_ceph(tmpdir):
    backup = Backup(str(tmpdir))
    backup.init('ceph-rbd', 'test/test04')
    config = open(str(tmpdir/'config')).read()
    config = json.loads(config)
    assert config == {
        "source-type": "ceph-rbd",
        "source": {"image": "test04", "pool": "test"},
        "schedule": {"daily": {"interval": "1d",
                               "keep": 9},
                     "weekly": {"interval": "7d",
                                "keep": 5},
                     "monthly": {"interval": "30d",
                                 "keep": 4}}}


def test_init_file(tmpdir):
    backup = Backup(str(tmpdir))
    backup.init('file', '/dev/foo')

    config = open(str(tmpdir/'config')).read()
    config = json.loads(config)
    assert config == {
        "source-type": "file",
        "source": {"filename": "/dev/foo"},
        "schedule": {"daily": {"interval": "1d",
                               "keep": 9},
                     "weekly": {"interval": "7d",
                                "keep": 5},
                     "monthly": {"interval": "30d",
                                 "keep": 4}}}


def test_init_creates_directory(tmpdir):
    target = str(tmpdir / 'asdf')
    assert not os.path.exists(target)
    backup = Backup(target)
    backup.init('file', '/dev/foo')
    assert os.path.isdir(target)


def test_init_refused_with_existing_config(tmpdir):
    existing_config = b"""\
{"source-type": "file", "source": {"filename": "asdf"}, "marker": 1}\
"""
    with open(str(tmpdir/'config'), 'wb') as f:
        f.write(existing_config)
    backup = Backup(str(tmpdir))
    with pytest.raises(RuntimeError):
        backup.init('file', '/dev/foo')
    with open(str(tmpdir/'config'), 'rb') as f:
        assert f.read() == existing_config
