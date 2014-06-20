from backy.backup import Backup
from backy.revision import Revision
import json
import mock
import os.path
import time


SAMPLE_DIR = os.path.join(os.path.dirname(__file__), 'samples')


def test_revision_base():
    backup = mock.Mock()
    revision = Revision('uuid', backup)
    assert revision.uuid == 'uuid'
    assert revision.backup is backup


def test_revision_create():
    backup = mock.Mock()
    backup.now = time.time
    backup.revision_history = []
    r = Revision.create(backup)
    assert r.uuid is not None
    assert time.time() - r.timestamp < 10
    assert r.backup is backup


def test_revision_uuids_sort_lexicographically_by_time():
    backup = mock.Mock()
    backup.now = time.time
    backup.revision_history = []
    rev = [Revision.create(backup) for _ in range(3)]
    for i in range(len(rev) - 1):
        assert str(rev[i].uuid) < str(rev[i + 1].uuid)


def test_revision_create_child():
    backup = mock.Mock()
    backup.now = time.time
    backup.revision_history = [Revision('asdf', backup)]
    r = Revision.create(backup)
    assert r.uuid is not None
    assert r.parent == 'asdf'
    assert time.time() - r.timestamp < 10
    assert r.backup is backup


def test_load_sample1():
    backup = mock.Mock()
    r = Revision.load(SAMPLE_DIR + '/sample1.rev', backup)
    assert r.uuid == 'asdf'
    assert r.timestamp == 20
    assert r.parent is None
    assert r.backup is backup


def test_load_sample2():
    backup = mock.Mock()
    r = Revision.load(SAMPLE_DIR + '/sample2.rev', backup)
    assert r.uuid == 'asdf2'
    assert r.timestamp == 21
    assert r.parent == 'asdf'
    assert r.backup is backup


def test_filenames_based_on_uuid_and_backup_dir():
    backup = mock.Mock()
    backup.path = '/srv/backup/foo'
    r = Revision('asdf', backup)
    assert r.filename == '/srv/backup/foo/asdf'
    assert r.info_filename == '/srv/backup/foo/asdf.rev'


def test_store_revision_data(tmpdir):
    backup = Backup(str(tmpdir))
    r = Revision('asdf2', backup)
    r.timestamp = 25
    r.parent = 'asdf'
    r.backup = backup
    r.write_info()
    info = open(r.info_filename, 'rb').read()
    assert json.loads(info.decode('utf-8')) == {
        "parent": "asdf",
        "uuid": "asdf2",
        "stats": {"bytes_written": 0},
        "tags": [],
        "timestamp": 25}
