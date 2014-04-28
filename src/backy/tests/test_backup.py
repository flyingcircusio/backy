from backy.backup import Backup
from backy.sources.file import File
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
    assert backup.revisions == {}
    assert backup.revision_history == []


def test_find_revision_empty(simple_file_config):
    backup = simple_file_config

    backup._scan_revisions()

    with pytest.raises(KeyError):
        backup.find_revision(-1)

    with pytest.raises(KeyError):
        backup.find_revision("last")

    with pytest.raises(KeyError):
        backup.find_revision("fdasfdka")
