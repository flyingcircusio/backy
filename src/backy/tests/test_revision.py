from backy.archive import Archive
from backy.backup import Backup
from backy.revision import Revision
import backy
import datetime
from unittest import mock
import os.path as p
import pytz
import yaml


SAMPLE_DIR = p.join(p.dirname(__file__), 'samples')


def archive():
    return mock.MagicMock(Archive)


def test_revision_base(archive):
    revision = Revision('uuid', archive)
    assert revision.uuid == 'uuid'
    assert revision.archive is archive


def test_revision_create(archive):
    archive.history = []
    r = Revision.create(archive)
    assert r.uuid is not None
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.archive is archive


def test_revision_create_child(archive):
    archive.history = [Revision('asdf', archive)]
    r = Revision.create(archive)
    assert r.uuid is not None
    assert r.parent == 'asdf'
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.archive is archive


def test_load_sample1(archive):
    r = Revision.load(SAMPLE_DIR + '/sample1.rev', archive)
    assert r.uuid == 'asdf'
    assert r.timestamp == datetime.datetime(2015, 8, 1, 20, 0, tzinfo=pytz.UTC)
    assert r.parent is None
    assert r.archive is archive


def test_load_sample2(archive):
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
    r = Revision('asdf2', backup, backy.utils.now())
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


def test_delete_revision(tmpdir):
    a = Archive(str(tmpdir))
    r = Revision('123-456', a, backy.utils.now())
    r.materialize()
    assert p.exists(str(tmpdir / '123-456'))
    assert p.exists(str(tmpdir / '123-456.rev'))
    a.scan()
    r.remove()
    assert not p.exists(str(tmpdir / '123-456'))
    assert not p.exists(str(tmpdir / '123-456.rev'))
