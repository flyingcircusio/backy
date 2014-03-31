from backy.backup import Revision, FullRevision, DeltaRevision, Backup
import mock
import os.path
import pytest
import shutil
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


def test_scrub_full_consistent(tmpdir):
    shutil.copytree(SAMPLE_DIR + '/scrub-consistent/',
                    str(tmpdir / 'backup'))
    backup = Backup(str(tmpdir / 'backup'))
    revision = backup.find_revision('528f6c52-d4a6-4237-806a-78207688f511')
    assert revision.type == 'full'
    result = revision.scrub()
    assert result == []


def test_scrub_delta_consistent(tmpdir):
    shutil.copytree(SAMPLE_DIR + '/scrub-consistent/',
                    str(tmpdir / 'backup'))
    backup = Backup(str(tmpdir / 'backup'))
    revision = backup.find_revision('de8285c3-af53-4967-a257-e66ea24f1020')
    assert revision.type == 'delta'
    result = revision.scrub()
    assert result == []


def test_scrub_full_inconsistent(tmpdir, capsys):
    shutil.copytree(SAMPLE_DIR + '/scrub-inconsistent/',
                    str(tmpdir / 'backup'))
    backup = Backup(str(tmpdir / 'backup'))
    revision = backup.find_revision('528f6c52-d4a6-4237-806a-78207688f511')
    assert revision.type == 'full'
    result = revision.scrub()
    assert result == [1]
    backup._scan()
    revision = backup.find_revision('528f6c52-d4a6-4237-806a-78207688f511')
    assert revision.blocksums == {
        0: 'e3b3dd4af7830bc49936c432757c0703',
        1: 'bad:6ff52182dd6928ffb59e26615e505213',
        2: 'f7e83d96381f32d558b532a3b22643ba',
        3: '02d5693204eb61a9f1c234eef3d3b880'}
    # running a second time shows that they have been marked already
    result = revision.scrub()
    assert result == [1]

    out, err = capsys.readouterr()
    assert """\
Scrubbing 528f6c52-d4a6-4237-806a-78207688f511 ...
Chunk 000001 is corrupt
Marking corrupt chunks.
Scrubbing 528f6c52-d4a6-4237-806a-78207688f511 ...
Chunk 000001 is known as corrupt.
Marking corrupt chunks.
""" == out


def test_scrub_delta_inconsistent(tmpdir, capsys):
    shutil.copytree(SAMPLE_DIR + '/scrub-inconsistent/',
                    str(tmpdir / 'backup'))
    backup = Backup(str(tmpdir / 'backup'))
    revision = backup.find_revision('de8285c3-af53-4967-a257-e66ea24f1020')
    assert revision.type == 'delta'
    result = revision.scrub()
    assert result == [1]
    backup._scan()
    revision = backup.find_revision('de8285c3-af53-4967-a257-e66ea24f1020')
    assert revision.blocksums == {1: u'bad:18d1ff02f24fdcd6dd680e93ba11920d'}
    # running a second time shows that they have been marked already
    result = revision.scrub()
    assert result == [1]

    out, err = capsys.readouterr()
    assert """\
Scrubbing de8285c3-af53-4967-a257-e66ea24f1020 ...
Chunk 000001 is corrupt
Marking corrupt chunks.
Scrubbing de8285c3-af53-4967-a257-e66ea24f1020 ...
Chunk 000001 is known as corrupt.
Marking corrupt chunks.
""" == str(out)
