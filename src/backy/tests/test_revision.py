from backy.backup import Backup
from backy.revision import Revision
import backy
import datetime
from unittest import mock
import os.path
import pytz
import yaml


SAMPLE_DIR = os.path.join(os.path.dirname(__file__), 'samples')


def test_revision_base():
    archive = mock.Mock()
    revision = Revision('uuid', archive)
    assert revision.uuid == 'uuid'
    assert revision.archive is archive


def test_revision_create():
    archive = mock.Mock()
    archive.history = []
    r = Revision.create(archive)
    assert r.uuid is not None
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.archive is archive


def test_revision_create_child():
    archive = mock.Mock()
    archive.history = [Revision('asdf', archive)]
    r = Revision.create(archive)
    assert r.uuid is not None
    assert r.parent == 'asdf'
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.archive is archive


def test_load_sample1():
    archive = mock.Mock()
    r = Revision.load(SAMPLE_DIR + '/sample1.rev', archive)
    assert r.uuid == 'asdf'
    assert r.timestamp == datetime.datetime(2015, 8, 1, 20, 0, tzinfo=pytz.UTC)
    assert r.parent is None
    assert r.archive is archive


def test_load_sample2():
    archive = mock.Mock()
    r = Revision.load(SAMPLE_DIR + '/sample2.rev', archive)
    assert r.uuid == 'asdf2'
    assert r.timestamp == datetime.datetime(2015, 8, 1, 21, 0, tzinfo=pytz.UTC)
    assert r.parent == 'asdf'
    assert r.archive is archive


def test_filenames_based_on_uuid_and_backup_dir():
    backup = mock.Mock()
    backup.path = '/srv/backup/foo'
    r = Revision('asdf', backup)
    assert r.filename == '/srv/backup/foo/asdf'
    assert r.info_filename == '/srv/backup/foo/asdf.rev'


def test_store_revision_data(tmpdir, clock):
    backup = Backup(str(tmpdir))
    r = Revision('asdf2', backup)
    r.timestamp = backy.utils.now()
    r.parent = 'asdf'
    r.backup = backup
    r.write_info()
    with open(r.info_filename, encoding='utf-8') as info:
        assert yaml.load(info) == {
            "parent": "asdf",
            "uuid": "asdf2",
            "stats": {"bytes_written": 0},
            "tags": [],
            "timestamp": datetime.datetime(2015, 9, 1, 7, 6, 47)}
