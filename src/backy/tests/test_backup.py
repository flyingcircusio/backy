from backy.backup import Backup
from backy.sources.file import File
import yaml
import os.path
import pytest
import shutil


fixtures = os.path.dirname(__file__) + '/samples'


@pytest.fixture
def simple_file_config(tmpdir):
    shutil.copy(fixtures + '/simple_file/config', str(tmpdir))
    b = Backup(str(tmpdir))
    b.configure()
    return b


def test_config(simple_file_config, tmpdir):
    backup = simple_file_config

    assert backup.path == str(tmpdir)
    assert isinstance(backup.source, File)
    assert backup.source.filename == "input-file"


def test_init_ceph(tmpdir):
    backup = Backup(str(tmpdir))
    backup.init('ceph-rbd', 'test/test04')
    with open(str(tmpdir / 'config')) as f:
        config = yaml.safe_load(f)
    assert config == {
        "type": "ceph-rbd",
        "image": "test04",
        "pool": "test"}


def test_init_file(tmpdir):
    backup = Backup(str(tmpdir))
    backup.init('file', '/dev/foo')

    with open(str(tmpdir / 'config')) as f:
        config = yaml.load(f)
    assert config == {
        "type": "file",
        "filename": "/dev/foo"}


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
    with open(str(tmpdir / 'config'), 'wb') as f:
        f.write(existing_config)
    backup = Backup(str(tmpdir))
    with pytest.raises(RuntimeError):
        backup.init('file', '/dev/foo')
    with open(str(tmpdir / 'config'), 'rb') as f:
        assert f.read() == existing_config
