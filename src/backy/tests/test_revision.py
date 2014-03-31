from backy.backup import Revision, FullRevision, DeltaRevision, Backup
import mock
import os.path
import pytest
import time


SAMPLE_DIR = os.path.join(os.path.dirname(__file__), 'samples')


def test_revision_base():
    backup = mock.Mock()
    revision = Revision('asdf', backup)
    assert revision.uuid == 'asdf'
    assert revision.backup is backup


def test_revision_select_type():
    assert Revision.select_type('full') == FullRevision
    assert Revision.select_type('delta') == DeltaRevision
    with pytest.raises(ValueError):
        Revision.select_type('foobar')


def test_revision_create_full():
    backup = mock.Mock()
    r = Revision.create('full', backup)
    assert r.uuid is not None
    assert time.time() - r.timestamp < 10
    assert r.backup is backup


def test_revision_create_delta():
    backup = mock.Mock()
    r = Revision.create('delta', backup)
    assert isinstance(r, DeltaRevision)
    assert r.uuid is not None
    assert time.time() - r.timestamp < 10
    assert r.backup is backup


def test_load_full_sample1():
    backup = mock.Mock()
    r = Revision.load(SAMPLE_DIR + '/full_sample1.backy', backup)
    assert r.uuid == 'asdf'
    assert r.timestamp == 20
    assert r.checksum == '6r7tf97ewt'
    assert r.parent is None
    assert r.blocksums == {0: u'asdf', 1: u'asdf', 2: u'asdf'}
    assert r.blocks == 3
    assert r.backup is backup


def test_load_delta_sample1():
    backup = mock.Mock()
    r = Revision.load(SAMPLE_DIR + '/delta_sample1.backy', backup)
    assert r.uuid == 'asdf2'
    assert r.timestamp == 21
    assert r.checksum == '6r7tf97ewt'
    assert r.parent == 'asdf'
    assert r.blocksums == {0: u'asdf'}
    assert r.blocks == 3
    assert r.backup is backup


def test_filenames_based_on_uuid_and_backup_dir():
    backup = mock.Mock()
    backup.path = '/srv/backup/foo'
    r = Revision('asdf', backup)
    assert r.filename == '/srv/backup/foo/asdf'
    assert r.info_filename == '/srv/backup/foo/asdf.backy'


def test_store_full_revision_data(tmpdir):
    backup = Backup(str(tmpdir))
    r = Revision.create('full', backup)
    r.uuid = 'asdf2'
    r.timestamp = 25
    r.checksum = 'cbd6de6dee'
    r.parent = None
    r.blocksums = {0: u'asdf'}
    r.blocks = 3
    r.backup = backup
    r.write_info()
    info = open(r.info_filename, 'rb').read()
    assert info == """\
{"parent": null, "blocks": 3, "uuid": "asdf2", "blocksums": {"0": "asdf"}, \
"timestamp": 25, "checksum": "cbd6de6dee", "type": "full"}\
"""


def test_store_delta_revision_data(tmpdir):
    backup = Backup(str(tmpdir))
    r = Revision.create('delta', backup)
    r.uuid = 'asdf2'
    r.timestamp = 25
    r.checksum = 'cbd6de6dee'
    r.parent = 'asdf'
    r.blocksums = {0: u'asdf'}
    r.blocks = 3
    r.backup = backup
    r.write_info()
    info = open(r.info_filename, 'rb').read()
    assert info == """\
{"parent": "asdf", "blocks": 3, "uuid": "asdf2", "blocksums": {"0": "asdf"}, \
"timestamp": 25, "checksum": "cbd6de6dee", "type": "delta"}\
"""
