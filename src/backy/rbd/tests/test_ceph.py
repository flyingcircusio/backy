import datetime
import io
import json
import subprocess
import time
from pathlib import Path
from unittest import mock

import consulate
import pytest

import backy.utils
from backy.rbd import RBDSource
from backy.rbd.rbd import RBDDiffV1
from backy.rbd.source import CephRBD
from backy.revision import Revision

BLOCK = backy.utils.PUNCH_SIZE

with open(Path(__file__).parent / "nodata.rbddiff", "rb") as f:
    SAMPLE_RBDDIFF = f.read()


@pytest.fixture
def check_output(monkeypatch):
    check_output = mock.Mock()
    check_output.return_value = b"{}"
    monkeypatch.setattr(subprocess, "check_output", check_output)
    return check_output


@pytest.fixture
def ceph_rbd(rbdclient, nosleep, log):
    """Provides a CephRBD object configured for image pool/test, with rbd
    being mocked away and allowing snapshots on that image."""
    ceph_rbd = CephRBD("test", "foo", log)
    # rbdclient mock setup:
    rbdclient._ceph_cli._register_image_for_snaps("test/foo")
    ceph_rbd.rbd = rbdclient
    return ceph_rbd


@pytest.fixture
def rbdsource(ceph_rbd, repository, log):
    return RBDSource(repository, ceph_rbd, log)


@pytest.fixture
def nosleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


def test_no_consul(ceph_rbd, monkeypatch):
    consul_class = mock.Mock()
    monkeypatch.setattr(consulate, "Consul", consul_class)

    ceph_rbd.create_snapshot("asdf")

    consul_class.assert_not_called()


def test_assign_revision(nosleep, log):
    ceph_rbd = CephRBD("test", "foo", log)
    revision = mock.Mock()
    context_manager = ceph_rbd(revision)
    assert context_manager.revision is revision


def test_context_manager(ceph_rbd, repository, log):
    """The imagesource context manager around a backup revision must create a
    corresponding snapshot at enter, and clean up at exit."""

    revision = Revision.create(repository, set(), log, uuid="1")
    with ceph_rbd(revision):
        assert ceph_rbd.rbd.snap_ls("test/foo")[0]["name"] == "backy-1"

    assert len(ceph_rbd.rbd.snap_ls("test/foo")) == 0


def test_context_manager_cleans_out_snapshots(ceph_rbd, repository, log):
    """The imagesource context manager cleans up unexpected backy snapshot revisions.
    Snapshots without the prefix 'backy-' are left untouched."""

    # snaps without backy- prefix are left untouched
    ceph_rbd.rbd.snap_create("test/foo@someother")
    # unexpected revision snapshots are cleaned
    ceph_rbd.rbd.snap_create("test/foo@backy-2")

    revision = Revision.create(repository, set(), log, uuid="1")
    revision.materialize()
    repository.scan()
    with ceph_rbd(revision):
        pass

    assert ceph_rbd.rbd.snap_ls("test/foo") == [
        {
            "id": 86925,
            "name": "someother",
            "protected": "false",
            "size": 32212254720,
            "timestamp": "Sun Feb 12 18:35:18 2023",
        },
        {
            "id": 86925,
            "name": "backy-1",
            "protected": "false",
            "size": 32212254720,
            "timestamp": "Sun Feb 12 18:35:18 2023",
        },
    ]


def test_choose_full_without_parent(ceph_rbd, repository, log):
    """When backing up a revision without a parent, a full backup needs to happen.
    The diff function must not be called."""

    revision = Revision.create(repository, set(), log)

    with ceph_rbd(revision) as s:
        assert not s.get_parent()


def test_choose_full_without_snapshot(ceph_rbd, repository, log):
    """When backing up a revision with an immediate parent that has no corresponding
    snapshot, that parent must be ignored and a full backup has to be made.
    The diff function must not be called."""

    revision1 = Revision.create(repository, set(), log)
    revision1.materialize()

    repository.scan()

    revision2 = Revision.create(repository, set(), log)

    with ceph_rbd(revision2) as s:
        assert not s.get_parent()


def test_choose_diff_with_snapshot(ceph_rbd, repository, log):
    """In an environment where a parent revision exists and has a snapshot, both
    revisions shall be diffed."""

    revision1 = Revision.create(repository, set(), log, uuid="a1")
    revision1.materialize()

    # part of test setup: we check backy's behavior when a previous version not only
    # exists, but also has a snapshot
    ceph_rbd.rbd.snap_create("test/foo@backy-a1")

    repository.scan()

    revision2 = Revision.create(repository, set(), log)

    with ceph_rbd(revision2) as s:
        assert s.get_parent().uuid == revision1.uuid


def test_diff_backup(ceph_rbd, rbdsource, repository, tmp_path, log):
    """When doing a diff backup between two revisions with snapshot, the RBDDiff needs
    to be called properly, a snapshot for the new revision needs to be created and the
    snapshot of the previous revision needs to be removed after the successfull backup."""

    parent = Revision.create(
        repository, set(), log, uuid="ed968696-5ab0-4fe0-af1c-14cadab44661"
    )
    parent.materialize()

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(
        repository, set(), log, uuid="f0e7292e-4ad8-4f2e-86d6-f40dca2aa802"
    )
    revision.timestamp = backy.utils.now() + datetime.timedelta(seconds=1)

    with rbdsource.open(parent) as f:
        f.write(b"asdf")

    repository.scan()
    revision.materialize()

    # test setup: ensure that previous revision has a snapshot. It needs to be removed
    # by the backup process
    ceph_rbd.rbd.snap_create(
        "test/foo@backy-ed968696-5ab0-4fe0-af1c-14cadab44661"
    )

    with mock.patch("backy.rbd.rbd.RBDClient.export_diff") as export:
        export.return_value = mock.MagicMock()
        export.return_value.__enter__.return_value = RBDDiffV1(
            io.BytesIO(SAMPLE_RBDDIFF)
        )
        with ceph_rbd(revision), rbdsource.open(revision) as f:
            ceph_rbd.diff(f, revision.get_parent())
            repository.history.append(revision)
        export.assert_called_with(
            "test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802",
            "backy-ed968696-5ab0-4fe0-af1c-14cadab44661",
        )

    current_snaps = ceph_rbd.rbd.snap_ls("test/foo")
    assert len(current_snaps) == 1
    assert (
        current_snaps[0]["name"] == "backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802"
    )


def test_full_backup(ceph_rbd, rbdsource, repository, tmp_path, log):
    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(repository, set(), log, uuid="a0")
    revision.materialize()
    repository.scan()

    with mock.patch("backy.rbd.rbd.RBDClient.export") as export:
        export.return_value = io.BytesIO(b"Han likes Leia.")
        with ceph_rbd(revision), rbdsource.open(revision) as f:
            ceph_rbd.full(f)
        export.assert_called_with("test/foo@backy-a0")

    # the corresponding snapshot for revision a0 is created by the backup process
    assert ceph_rbd.rbd.snap_ls("test/foo")[0]["name"] == "backy-a0"

    with rbdsource.open(revision) as f:
        assert f.read() == b"Han likes Leia."

    # Now make another full backup. This overwrites the first.
    revision2 = Revision.create(repository, set(), log, uuid="a1")
    revision2.timestamp = backy.utils.now() + datetime.timedelta(seconds=1)
    revision2.materialize()
    repository.scan()

    with mock.patch("backy.rbd.rbd.RBDClient.export") as export:
        export.return_value = io.BytesIO(b"Han loves Leia.")
        with ceph_rbd(revision2), rbdsource.open(revision2) as f:
            ceph_rbd.full(f)

    with rbdsource.open(revision2) as f:
        assert f.read() == b"Han loves Leia."

    current_snaps = ceph_rbd.rbd.snap_ls("test/foo")
    assert len(current_snaps) == 1
    assert current_snaps[0]["name"] == "backy-a1"


def test_full_backup_integrates_changes(
    ceph_rbd, rbdsource, repository, tmp_path, log
):
    # The backup source changes between two consecutive full backups. Both
    # backup images should reflect the state of the source at the time the
    # backup was run. This test is here to detect regressions while optimizing
    # the full backup algorithms (copying and applying deltas).
    content0 = BLOCK * b"A" + BLOCK * b"B" + BLOCK * b"C" + BLOCK * b"D"
    content1 = BLOCK * b"A" + BLOCK * b"X" + BLOCK * b"\0" + BLOCK * b"D"

    rev0 = Revision.create(repository, set(), log)
    rev0.materialize()
    repository.scan()

    rev1 = Revision.create(repository, set(), log)
    rev1.timestamp = backy.utils.now() + datetime.timedelta(seconds=1)
    rev1.materialize()

    # check fidelity
    for content, rev in [(content0, rev0), (content1, rev1)]:
        with mock.patch("backy.rbd.rbd.RBDClient.export") as export:
            export.return_value = io.BytesIO(content)
            with ceph_rbd(rev), rbdsource.open(rev) as target:
                ceph_rbd.full(target)
            export.assert_called_with("test/foo@backy-{}".format(rev.uuid))

        with rbdsource.open(rev) as f:
            assert content == f.read()


def test_verify_fail(ceph_rbd, rbdsource, repository, tmp_path, log):
    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(repository, set(), log)
    revision.materialize()

    repository.scan()

    rbd_source = str(tmp_path / "-dev-rbd0")
    with open(rbd_source, "w") as f:
        f.write("Han likes Leia.")

    with rbdsource.open(revision) as f:
        f.write(b"foobar")
    # The chunked store has false data, so this needs to be detected.
    with ceph_rbd(revision), rbdsource.open(revision) as target:
        assert ceph_rbd.verify(target)


def test_verify(ceph_rbd, rbdsource, repository, tmp_path, log):
    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(repository, set(), log, uuid="a0")
    revision.materialize()

    repository.scan()

    rbd_source = ceph_rbd.rbd.map("test/foo@backy-a0")["device"]
    with open(rbd_source, "wb") as f:
        f.write(b"Han likes Leia.")
    ceph_rbd.rbd.unmap(rbd_source)

    with rbdsource.open(revision) as f:
        f.write(b"Han likes Leia.")
        f.flush()

    with ceph_rbd(revision), rbdsource.open(revision) as target:
        assert not ceph_rbd.verify(target)


@pytest.fixture
def fcrd(log):
    return CephRBD(
        "test",
        "test01.root",
        log,
        "test01",
        "12345",
    )


def test_flyingcircus_source(fcrd):
    assert fcrd.pool == "test"
    assert fcrd.image == "test01.root"
    assert fcrd.vm == "test01"
    assert fcrd.consul_acl_token == "12345"


@pytest.mark.slow
def test_flyingcircus_consul_interaction(monkeypatch, fcrd):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = ConsulKVDict()
    monkeypatch.setattr(consulate, "Consul", consul_class)

    check_output = mock.Mock()
    check_output.side_effect = ["[]", '[{"name": "asdf"}]']
    monkeypatch.setattr(subprocess, "check_output", check_output)
    fcrd.create_snapshot("asdf")


class ConsulKVDict(dict):
    def __setitem__(self, k, v):
        if not isinstance(v, bytes):
            v = json.dumps(v)
        super(ConsulKVDict, self).__setitem__(k, v)

    def find(self, prefix):
        for key in self:
            if key.startswith(prefix):
                yield key


@pytest.mark.slow
def test_flyingcircus_consul_interaction_timeout(monkeypatch, fcrd):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = ConsulKVDict()
    monkeypatch.setattr(consulate, "Consul", consul_class)

    check_output = mock.Mock()
    check_output.side_effect = [
        '[{"name": "bsdf"}]',
        "[]",
        "[]",
        "[]",
        "[]",
        "[]",
    ]
    monkeypatch.setattr(subprocess, "check_output", check_output)

    fcrd.snapshot_timeout = 2
    fcrd.create_snapshot("asdf")

    assert check_output.call_args[0][0] == [
        "rbd",
        "snap",
        "create",
        "test/test01.root@asdf",
    ]
