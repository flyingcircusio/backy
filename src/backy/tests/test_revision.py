import datetime
import os.path as p
from unittest import mock

import yaml

import backy
from backy.revision import Revision

UTC = datetime.timezone.utc
SAMPLE_DIR = p.join(p.dirname(__file__), "samples")


def test_revision_base(backup, log):
    revision = Revision(backup, log, "uuid")
    assert revision.uuid == "uuid"
    assert revision.backup is backup


def test_revision_create(backup, log):
    backup.history = []
    r = Revision.create(backup, {"1", "2"}, log)
    assert r.uuid is not None
    assert r.tags == set(["1", "2"])
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.backup is backup


def test_revision_create_child(backup, log):
    backup.history = [Revision(backup, log, "asdf")]
    r = Revision.create(backup, {"test"}, log)
    assert r.uuid is not None
    assert r.tags == {"test"}
    assert r.get_parent().uuid == "asdf"
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.backup is backup


def test_load_sample1(backup, log):
    r = Revision.load(SAMPLE_DIR + "/sample1.rev", backup, log)
    assert r.uuid == "asdf"
    assert r.timestamp == datetime.datetime(2015, 8, 1, 20, 0, tzinfo=UTC)
    assert r.get_parent() is None
    assert r.backup is backup


def test_load_sample2(backup, log):
    r = Revision.load(SAMPLE_DIR + "/sample2.rev", backup, log)
    assert r.uuid == "asdf2"
    assert r.timestamp == datetime.datetime(2015, 8, 1, 21, 0, tzinfo=UTC)
    assert r.get_parent() is None
    assert r.backup is backup


def test_filenames_based_on_uuid_and_backup_dir(log):
    backup = mock.Mock()
    backup.path = "/srv/backup/foo"
    r = Revision(backup, log, "asdf")
    assert r.filename == "/srv/backup/foo/asdf"
    assert r.info_filename == "/srv/backup/foo/asdf.rev"


def test_store_revision_data(backup, clock, log):
    backup.history = [Revision(backup, log, "asdf", backy.utils.now())]
    r = Revision(backup, log, "asdf2", backy.utils.now())
    r.backup = backup
    r.write_info()
    with open(r.info_filename, encoding="utf-8") as info:
        assert yaml.safe_load(info) == {
            "parent": "asdf",
            "backend_type": "chunked",
            "uuid": "asdf2",
            "stats": {"bytes_written": 0},
            "tags": [],
            "trust": "trusted",
            "timestamp": datetime.datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC),
        }


def test_store_revision_data_no_parent(backup, clock, log):
    r = Revision(backup, log, "asdf2", backy.utils.now())
    r.backup = backup
    r.write_info()
    with open(r.info_filename, encoding="utf-8") as info:
        assert yaml.safe_load(info) == {
            "parent": "",
            "backend_type": "chunked",
            "uuid": "asdf2",
            "stats": {"bytes_written": 0},
            "tags": [],
            "trust": "trusted",
            "timestamp": datetime.datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC),
        }


def test_delete_revision(backup, log):
    r = Revision(backup, log, "123-456", backy.utils.now())
    r.materialize()
    assert p.exists(backup.path + "/123-456.rev")
    backup.scan()
    open(backup.path + "/123-456", "w")
    assert p.exists(backup.path + "/123-456.rev")
    r.remove()
    # Ensure the revision data file exists - we do not implicitly create
    # it any longer.
    assert not p.exists(backup.path + "/123-456")
    assert not p.exists(backup.path + "/123-456.rev")
