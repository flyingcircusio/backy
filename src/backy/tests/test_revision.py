import datetime
from pathlib import Path
from unittest import mock

import yaml

import backy.utils
from backy.revision import Revision

UTC = datetime.timezone.utc
SAMPLE_DIR = Path(__file__).parent.joinpath("samples")


def test_revision_base(backup, log):
    revision = Revision.create(backup, set(), log, uuid="uuid")
    assert revision.uuid == "uuid"
    assert revision.backup is backup


def test_revision_create(backup, log):
    backup.history = []
    r = Revision.create(backup, {"1", "2"}, log)
    assert r.uuid is not None
    assert r.tags == {"1", "2"}
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.backup is backup


def test_revision_create_child(backup, log):
    backup.history = [Revision.create(backup, set(), log, uuid="asdf")]
    r = Revision.create(backup, {"test"}, log)
    assert r.uuid is not None
    assert r.tags == {"test"}
    assert r.get_parent().uuid == "asdf"
    assert (backy.utils.now() - r.timestamp).total_seconds() < 10
    assert r.backup is backup


def test_load_sample1(backup, log):
    r = Revision.load(SAMPLE_DIR / "sample1.rev", backup, log)
    assert r.uuid == "asdf"
    assert r.timestamp == datetime.datetime(2015, 8, 1, 20, 0, tzinfo=UTC)
    assert r.get_parent() is None
    assert r.backup is backup


def test_load_sample2(backup, log):
    r = Revision.load(SAMPLE_DIR / "sample2.rev", backup, log)
    assert r.uuid == "asdf2"
    assert r.timestamp == datetime.datetime(2015, 8, 1, 21, 0, tzinfo=UTC)
    assert r.get_parent() is None
    assert r.backup is backup


def test_filenames_based_on_uuid_and_backup_dir(log):
    backup = mock.Mock()
    backup.path = Path("/srv/backup/foo")
    r = Revision.create(backup, set(), log, uuid="asdf")
    assert r.filename == Path("/srv/backup/foo/asdf")
    assert r.info_filename == Path("/srv/backup/foo/asdf.rev")


def test_store_revision_data(backup, clock, log):
    backup.history = [Revision.create(backup, set(), log, uuid="asdf")]
    r = Revision.create(backup, set(), log, uuid="asdf2")
    r.write_info()
    with open(r.info_filename, encoding="utf-8") as info:
        assert yaml.safe_load(info) == {
            "parent": "asdf",
            "backend_type": backup.default_backend_type,
            "uuid": "asdf2",
            "stats": {"bytes_written": 0},
            "tags": [],
            "orig_tags": [],
            "server": "",
            "trust": "trusted",
            "timestamp": datetime.datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC),
        }


def test_store_revision_data_no_parent(backup, clock, log):
    r = Revision.create(backup, set(), log, uuid="asdf2")
    r.write_info()
    with open(r.info_filename, encoding="utf-8") as info:
        assert yaml.safe_load(info) == {
            "parent": "",
            "backend_type": backup.default_backend_type,
            "uuid": "asdf2",
            "stats": {"bytes_written": 0},
            "tags": [],
            "orig_tags": [],
            "server": "",
            "trust": "trusted",
            "timestamp": datetime.datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC),
        }


def test_delete_revision(backup, log):
    r = Revision.create(backup, set(), log, uuid="123-456")
    r.materialize()
    assert backup.path.joinpath("123-456.rev").exists()
    backup.scan()
    backup.path.joinpath("123-456").open("w")
    assert backup.path.joinpath("123-456.rev").exists()
    r.remove()
    # Ensure the revision data file exists - we do not implicitly create
    # it any longer.
    assert not backup.path.joinpath("123-456").exists()
    assert not backup.path.joinpath("123-456.rev").exists()
