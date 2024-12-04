import os
import subprocess
from pathlib import Path
from typing import IO
from unittest import mock

import pytest

from backy.conftest import create_rev
from backy.ext_deps import BACKY_RBD_CMD, BASH
from backy.rbd import CephRBD, RBDRestoreArgs, RBDSource
from backy.source import CmdLineSource
from backy.tests import Ellipsis
from backy.utils import CHUNK_SIZE


class FakeCephRBD:
    data = ""

    def __init__(self, data):
        self.data = data

    def ready(self):
        return bool(self.data)

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_parent(self):
        return None

    def full(self, file):
        assert self.data
        file.write(self.data)

    def verify(self, target: IO, report=None):
        return self.data == target.read()


@pytest.fixture
def rbdsource(repository, log):
    return RBDSource(repository, FakeCephRBD(b""), log)


def test_configure_rbd_source_no_consul(repository, tmp_path, log):
    config = {
        "path": str(tmp_path),
        "schedule": {},
        "source": {
            "type": "rbd",
            "pool": "test",
            "image": "test04.root",
        },
    }
    source = CmdLineSource.from_config(config, log).create_source()
    assert isinstance(source, RBDSource)
    ceph_rbd = source.ceph_rbd
    assert isinstance(ceph_rbd, CephRBD)
    assert ceph_rbd.pool == "test"
    assert ceph_rbd.image == "test04.root"
    assert ceph_rbd.always_full is False
    assert ceph_rbd.vm is None
    assert ceph_rbd.consul_acl_token is None


def test_configure_rbd_source_consul(repository, tmp_path, log):
    config = {
        "path": str(tmp_path),
        "schedule": {},
        "source": {
            "type": "rbd",
            "pool": "test",
            "image": "test04.root",
            "full-always": True,
            "vm": "test04",
            "consul_acl_token": "token",
        },
    }
    source = CmdLineSource.from_config(config, log).create_source()
    assert isinstance(source, RBDSource)
    ceph_rbd = source.ceph_rbd
    assert isinstance(ceph_rbd, CephRBD)
    assert ceph_rbd.pool == "test"
    assert ceph_rbd.image == "test04.root"
    assert ceph_rbd.always_full is True
    assert ceph_rbd.vm == "test04"
    assert ceph_rbd.consul_acl_token == "token"


def test_restore_target(rbdsource, repository, tmp_path, log):
    data = b"volume contents\n"
    rbdsource.ceph_rbd.data = data
    target = tmp_path / "restore.img"
    r = create_rev(repository, {"daily"})
    rbdsource.backup(r)
    rbdsource.restore(r, RBDRestoreArgs(str(target)))
    with open(target, "rb") as t:
        assert data == t.read()


def test_restore_stdout(rbdsource, repository, capfd, log):
    data = b"volume contents\n"
    rbdsource.ceph_rbd.data = data
    r = create_rev(repository, {"daily"})
    rbdsource.backup(r)
    rbdsource.restore(r, RBDRestoreArgs("-"))
    assert not Path("-").exists()
    out, err = capfd.readouterr()
    assert data.decode("utf-8") == out


def test_restore_backy_extract(rbdsource, repository, monkeypatch, log):
    check_output = mock.Mock(return_value="backy-extract 1.1.0")
    monkeypatch.setattr(subprocess, "check_output", check_output)
    rbdsource.restore_backy_extract = mock.Mock()
    data = b"a" * CHUNK_SIZE
    rbdsource.ceph_rbd.data = data
    r = create_rev(repository, {"daily"})
    rbdsource.backup(r)
    rbdsource.restore(r, RBDRestoreArgs("restore.img"))
    check_output.assert_called()
    rbdsource.restore_backy_extract.assert_called_once_with(r, "restore.img")


def test_backup_corrupted(rbdsource, repository, log):
    data = b"volume contents\n"
    rbdsource.ceph_rbd.data = data
    r = create_rev(repository, {"daily"})
    rbdsource.backup(r)

    chunk_path = rbdsource.store.chunk_path(next(iter(rbdsource.store.seen)))
    chunk_path.chmod(0o664)
    with open(chunk_path, "wb") as f:
        f.write(b"invalid")
    r2 = create_rev(repository, {"daily"})
    rbdsource.backup(r2)

    assert repository.history == []
    assert not chunk_path.exists()


def test_gc(rbdsource, repository, log):
    r = create_rev(repository, set())
    # Write 1 version to the file
    with rbdsource.open(r, "wb") as f:
        f.write(b"asdf")
    remote = create_rev(repository, set())  # remote revision without local data
    remote.server = "remote"
    remote.materialize()

    # Reassign as the scan will create a new reference
    r = repository.find_by_uuid(r.uuid)
    assert len(list(rbdsource.store.ls())) == 1
    rbdsource.gc()
    assert len(list(rbdsource.store.ls())) == 1
    r.remove()
    rbdsource.gc()
    assert len(list(rbdsource.store.ls())) == 0


def test_open_distrusted(rbdsource, repository):
    r1 = create_rev(repository, set())
    rbdsource.open(r1, "wb")
    assert not rbdsource.store.force_writes

    r1.distrust()
    r1.write_info()
    r2 = create_rev(repository, set())
    rbdsource.open(r2, "wb")
    assert rbdsource.store.force_writes


def test_smoketest_internal(rbdsource, repository, tmp_path, log):
    # These copies of data are intended to be different versions of the same
    # file.
    data1 = b"1" * 2 * 1024**2
    data2 = b"2" * 2 * 1024**2
    data3 = b"3" * 2 * 1024**2

    # Backup first state
    rbdsource.ceph_rbd.data = data1
    rev1 = create_rev(repository, {"manual:test"})
    rbdsource.backup(rev1)

    # Restore first state from the newest revision
    restore_args = RBDRestoreArgs(str(tmp_path / "image1.restore"))
    rbdsource.restore(rev1, restore_args)
    with pytest.raises(IOError):
        open(repository.history[-1].info_filename, "wb")
    assert data1 == open(restore_args.target, "rb").read()

    # Backup second state
    rbdsource.ceph_rbd.data = data2
    rev2 = create_rev(repository, {"test"})
    rbdsource.backup(rev2)
    assert len(repository.history) == 2

    # Restore second state from second backup which is the newest at position 0
    rbdsource.restore(rev2, restore_args)
    assert data2 == open(restore_args.target, "rb").read()

    # Our original backup is now at position 1. Lets restore that again.
    rbdsource.restore(rev1, restore_args)
    assert data1 == open(restore_args.target, "rb").read()

    # Backup second state again
    rbdsource.ceph_rbd.data = data2
    rev3 = create_rev(repository, {"manual:test"})
    rbdsource.backup(rev3)
    assert len(repository.history) == 3

    # Restore image2 from its most recent at position 0
    rbdsource.restore(rev3, restore_args)
    assert data2 == open(restore_args.target, "rb").read()

    # Restore image2 from its previous backup, now at position 1
    rbdsource.restore(rev2, restore_args)
    assert data2 == open(restore_args.target, "rb").read()

    # Our original backup is now at position 2. Lets restore that again.
    rbdsource.restore(rev1, restore_args)
    assert data1 == open(restore_args.target, "rb").read()

    # Backup third state
    rbdsource.ceph_rbd.data = data3
    rev4 = create_rev(repository, {"test"})
    rbdsource.backup(rev4)
    assert len(repository.history) == 4

    # Restore image3 from the most curent state
    rbdsource.restore(rev4, restore_args)
    assert data3 == open(restore_args.target, "rb").read()

    # Restore image2 from position 1 and 2
    rbdsource.restore(rev3, restore_args)
    assert data2 == open(restore_args.target, "rb").read()

    rbdsource.restore(rev2, restore_args)
    assert data2 == open(restore_args.target, "rb").read()

    # Restore image1 from position 3
    rbdsource.restore(rev1, restore_args)
    assert data1 == open(restore_args.target, "rb").read()


@pytest.mark.slow
@pytest.mark.skip
def test_smoketest_external():
    output = subprocess.check_output(
        [BASH, Path(__file__).parent / "smoketest.sh"],
        env=os.environ | {"BACKY_RBD_CMD": BACKY_RBD_CMD},
    )
    output = output.decode("utf-8")
    assert (
        Ellipsis(
            """\
Using /... as workspace.
Generating Test Data.. Done.
Backing up img_state1.img. Done.
Backing up img_state1.img with unknown tag. Done.
Restoring img_state1.img from level 0. Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state2.img. Done.
Restoring img_state2.img from level 0. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 1. Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state2.img again. Done.
Restoring img_state2.img from level 0. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state2.img from level 1. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 2. Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state3.img. Done.
Restoring img_state3.img from level 0. Done.
Diffing restore_state3.img against img_state3.img. Success.
Restoring img_state2.img from level 1. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state2.img from level 2. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 3. Done.
Diffing restore_state1.img against img_state1.img. Success.
┏━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
┃ Date      ┃           ┃           ┃          ┃            ┃         ┃        ┃
┃ ... ┃ ID        ┃      Size ┃ Duration ┃ Tags       ┃ Trust   ┃ Server ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
│ ... │ ... │ 512.0 KiB │ a moment │ manual:te… │ trusted │        │
│ ... │           │           │          │            │         │        │
│ ... │ ... │ 512.0 KiB │ a moment │ daily      │ trusted │        │
│ ... │           │           │          │            │         │        │
│ ... │ ... │ 512.0 KiB │ a moment │ test       │ trusted │        │
│ ... │           │           │          │            │         │        │
│ ... │ ... │ 512.0 KiB │ a moment │ manual:te… │ trusted │        │
│ ... │           │           │          │            │         │        │
└───────────┴───────────┴───────────┴──────────┴────────────┴─────────┴────────┘
4 revisions containing 2.0 MiB data (estimated)
┏━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
┃ Date      ┃           ┃           ┃          ┃            ┃         ┃        ┃
┃ ... ┃ ID        ┃      Size ┃ Duration ┃ Tags       ┃ Trust   ┃ Server ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
│ ... │ ... │ 512.0 KiB │ a moment │ manual:te… │ trusted │        │
│ ... │           │           │          │            │         │        │
│ ... │ ... │ 512.0 KiB │ a moment │ test       │ trusted │        │
│ ... │           │           │          │            │         │        │
│ ... │ ... │ 512.0 KiB │ a moment │ manual:te… │ trusted │        │
│ ... │           │           │          │            │         │        │
└───────────┴───────────┴───────────┴──────────┴────────────┴─────────┴────────┘
3 revisions containing 1.5 MiB data (estimated)
"""
        )
        == output
    )
