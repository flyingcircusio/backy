import datetime
import io
import os.path as p
import subprocess
import time
from unittest import mock

import pytest

import backy.utils
from backy.backends.chunked import ChunkedFileBackend
from backy.backends.cowfile import COWFileBackend
from backy.revision import Revision
from backy.sources import select_source
from backy.sources.ceph.source import CephRBD

BLOCK = backy.utils.PUNCH_SIZE

with open(p.join(p.dirname(__file__), "nodata.rbddiff"), "rb") as f:
    SAMPLE_RBDDIFF = f.read()


@pytest.fixture
def check_output(monkeypatch):
    check_output = mock.Mock()
    check_output.return_value = b"{}"
    monkeypatch.setattr(subprocess, "check_output", check_output)
    return check_output


@pytest.fixture
def ceph_rbd_imagesource(rbdclient, nosleep, log):
    """Provides a CephRBD object configured for image pool/test, with rbd
    being mocked away and allowing snapshots on that image."""
    source = CephRBD(dict(pool="test", image="foo"), log)
    # rbdclient mock setup:
    rbdclient._ceph_cli._register_image_for_snaps("test/foo")
    source.rbd = rbdclient
    return source


@pytest.fixture
def nosleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


def test_select_ceph_source():
    assert select_source("ceph-rbd") == CephRBD


def test_assign_revision(nosleep, log):
    source = CephRBD(dict(pool="test", image="foo"), log)
    revision = mock.Mock()
    context_manager = source(revision)
    assert context_manager.revision is revision


def test_context_manager(backup, ceph_rbd_imagesource, log):
    """The imagesource context manager around a backup revision must create a
    corresponding snapshot at enter, and clean up at exit."""
    source = ceph_rbd_imagesource

    revision = Revision.create(backup, set(), log, uuid="1")
    with source(revision):
        assert source.rbd.snap_ls("test/foo")[0]["name"] == "backy-1"

    assert len(source.rbd.snap_ls("test/foo")) == 0


def test_context_manager_cleans_out_snapshots(
    ceph_rbd_imagesource, backup, log
):
    """The imagesource context manager cleans up unexpected backy snapshot revisions.
    Snapshots without the prefix 'backy-' are left untouched."""
    source = ceph_rbd_imagesource

    # snaps without backy- prefix are left untouched
    source.rbd.snap_create("test/foo@someother")
    # unexpected revision snapshots are cleaned
    source.rbd.snap_create("test/foo@backy-2")

    revision = Revision.create(backup, set(), log, uuid="1")
    with source(revision):
        revision.materialize()
        backup.scan()

    assert source.rbd.snap_ls("test/foo") == [
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


def test_choose_full_without_parent(ceph_rbd_imagesource, backup, log):
    """When backing up a revision without a parent, a full backup needs to happen.
    The diff function must not be called."""
    source = ceph_rbd_imagesource

    source.diff = mock.Mock()
    source.full = mock.Mock()

    revision = Revision.create(backup, set(), log)

    with source(revision):
        source.backup(revision.backend)

    assert not source.diff.called
    assert source.full.called


def test_choose_full_without_snapshot(ceph_rbd_imagesource, backup, log):
    """When backing up a revision with an immediate parent that has no corresponding
    snapshot, that parent must be ignored and a full backup has to be made.
    The diff function must not be called."""
    source = ceph_rbd_imagesource

    source.diff = mock.Mock()
    source.full = mock.Mock()

    revision1 = Revision.create(backup, set(), log)
    revision1.materialize()

    backup.scan()

    revision2 = Revision.create(backup, set(), log)

    with source(revision2):
        source.backup(revision2.backend)

    assert not source.diff.called
    assert source.full.called


def test_choose_diff_with_snapshot(ceph_rbd_imagesource, backup, log):
    """In an environment where a parent revision exists and has a snapshot, both
    revisions shall be diffed."""
    source = ceph_rbd_imagesource

    source.diff = mock.Mock()
    source.full = mock.Mock()

    revision1 = Revision.create(backup, set(), log, uuid="a1")
    revision1.materialize()

    # part of test setup: we check backy's behavior when a previous version not only
    # exists, but also has a snapshot
    source.rbd.snap_create("test/foo@backy-a1")

    backup.scan()

    revision2 = Revision.create(backup, set(), log)

    with source(revision2):
        source.backup(revision2.backend)

    assert source.diff.called
    assert not source.full.called


def test_diff_backup(ceph_rbd_imagesource, backup, tmp_path, log):
    """When doing a diff backup between two revisions with snapshot, the RBDDiff needs
    to be called properly, a snapshot for the new revision needs to be created and the
    snapshot of the previous revision needs to be removed after the successfull backup."""
    from backy.sources.ceph.diff import RBDDiffV1

    source = ceph_rbd_imagesource

    parent = Revision.create(
        backup, set(), log, uuid="ed968696-5ab0-4fe0-af1c-14cadab44661"
    )
    parent.materialize()

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(
        backup, set(), log, uuid="f0e7292e-4ad8-4f2e-86d6-f40dca2aa802"
    )
    revision.timestamp = backy.utils.now() + datetime.timedelta(seconds=1)

    with parent.backend.open("wb") as f:
        f.write(b"asdf")

    backup.scan()
    revision.materialize()

    # test setup: ensure that previous revision has a snapshot. It needs to be removed
    # by the backup process
    source.rbd.snap_create(
        "test/foo@backy-ed968696-5ab0-4fe0-af1c-14cadab44661"
    )

    with mock.patch("backy.sources.ceph.rbd.RBDClient.export_diff") as export:
        export.return_value = mock.MagicMock()
        export.return_value.__enter__.return_value = RBDDiffV1(
            io.BytesIO(SAMPLE_RBDDIFF)
        )
        with source(revision):
            source.diff(revision.backend, revision.get_parent())
            backup.history.append(revision)
        export.assert_called_with(
            "test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802",
            "backy-ed968696-5ab0-4fe0-af1c-14cadab44661",
        )

    current_snaps = source.rbd.snap_ls("test/foo")
    assert len(current_snaps) == 1
    assert (
        current_snaps[0]["name"] == "backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802"
    )


def test_full_backup(ceph_rbd_imagesource, backup, tmp_path, log):
    source = ceph_rbd_imagesource

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(backup, set(), log, uuid="a0")
    revision.materialize()
    backup.scan()

    with mock.patch("backy.sources.ceph.rbd.RBDClient.export") as export:
        export.return_value = io.BytesIO(b"Han likes Leia.")
        backend = revision.backend
        with source(revision):
            source.full(backend)
        export.assert_called_with("test/foo@backy-a0")

    # the corresponding snapshot for revision a0 is created by the backup process
    assert source.rbd.snap_ls("test/foo")[0]["name"] == "backy-a0"

    with backend.open("rb") as f:
        assert f.read() == b"Han likes Leia."

    # Now make another full backup. This overwrites the first.
    revision2 = Revision.create(backup, set(), log, uuid="a1")
    revision2.timestamp = backy.utils.now() + datetime.timedelta(seconds=1)
    revision2.materialize()
    backup.scan()

    with mock.patch("backy.sources.ceph.rbd.RBDClient.export") as export:
        export.return_value = io.BytesIO(b"Han loves Leia.")
        backend = revision2.backend
        with source(revision2):
            source.full(backend)

    with backend.open("rb") as f:
        assert f.read() == b"Han loves Leia."

    current_snaps = source.rbd.snap_ls("test/foo")
    assert len(current_snaps) == 1
    assert current_snaps[0]["name"] == "backy-a1"


def test_full_backup_integrates_changes(
    ceph_rbd_imagesource, backup, tmp_path, log
):
    # The backup source changes between two consecutive full backups. Both
    # backup images should reflect the state of the source at the time the
    # backup was run. This test is here to detect regressions while optimizing
    # the full backup algorithms (copying and applying deltas).
    source = ceph_rbd_imagesource
    content0 = BLOCK * b"A" + BLOCK * b"B" + BLOCK * b"C" + BLOCK * b"D"
    content1 = BLOCK * b"A" + BLOCK * b"X" + BLOCK * b"\0" + BLOCK * b"D"

    rev0 = Revision.create(backup, set(), log)
    rev0.materialize()
    backup.scan()

    rev1 = Revision.create(backup, set(), log)
    rev1.timestamp = backy.utils.now() + datetime.timedelta(seconds=1)
    rev1.materialize()

    # check fidelity
    for content, rev in [(content0, rev0), (content1, rev1)]:
        with mock.patch("backy.sources.ceph.rbd.RBDClient.export") as export:
            export.return_value = io.BytesIO(content)
            with source(rev):
                source.full(rev.backend)
            export.assert_called_with("test/foo@backy-{}".format(rev.uuid))

        with rev.backend.open("rb") as f:
            assert content == f.read()


def test_verify_fail(backup, tmp_path, ceph_rbd_imagesource, log):
    source = ceph_rbd_imagesource

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(backup, set(), log)
    revision.materialize()

    backup.scan()

    rbd_source = str(tmp_path / "-dev-rbd0")
    with open(rbd_source, "w") as f:
        f.write("Han likes Leia.")

    backend = revision.backend
    with backend.open("wb") as f:
        f.write(b"foobar")
    # The backend has false data, so this needs to be detected.
    with source(revision):
        assert not source.verify(backend)
        assert len(backup.quarantine.report_ids) == 1


def test_verify(ceph_rbd_imagesource, backup, tmp_path, log):
    source = ceph_rbd_imagesource

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision.create(backup, set(), log, uuid="a0")
    revision.materialize()

    backup.scan()

    rbd_source = source.rbd.map("test/foo@backy-a0")["device"]
    with open(rbd_source, "wb") as f:
        f.write(b"Han likes Leia.")
    source.rbd.unmap(rbd_source)

    with revision.backend.open("wb") as f:
        f.write(b"Han likes Leia.")
        f.flush()

    with source(revision):
        assert source.verify(revision.backend)
