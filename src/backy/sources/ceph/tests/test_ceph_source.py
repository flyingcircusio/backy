import io
import os.path as p
import subprocess
import time
from unittest import mock

import pytest

import backy.utils
from backy.backends.chunked import ChunkedFileBackend
from backy.backends.cowfile import COWFileBackend
from backy.ext_deps import RBD
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
def nosleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


def test_select_ceph_source():
    assert select_source("ceph-rbd") == CephRBD


def test_assign_revision():
    source = CephRBD(dict(pool="test", image="foo"))
    revision = mock.Mock()
    context_manager = source(revision)
    assert context_manager.revision is revision


def test_context_manager(backup, rbdclient):

    source = CephRBD(dict(pool="test", image="foo"))
    # rbdclient mock setup:
    rbdclient._ceph_cli._register_image_for_snaps("test/foo")
    source.rbd = rbdclient

    revision = Revision(backup, 1)
    with source(revision):
        assert rbdclient.snap_ls("test/foo")[0]['name'] == "backy-1"

    assert len(rbdclient.snap_ls("test/foo")) == 0


def test_context_manager_cleans_out_snapshots(rbdclient, backup, nosleep):
    source = CephRBD(dict(pool="test", image="foo"))
    # rbdclient mock setup:
    rbdclient._ceph_cli._register_image_for_snaps("test/foo")
    source.rbd = rbdclient

    # snaps without backy- prefix are left untouched
    rbdclient.snap_create("test/foo@someother")
    # unexpected revision snapshots are cleaned
    rbdclient.snap_create("test/foo@backy-2")

    revision = Revision(backup, "1")
    with source(revision):
        revision.materialize()
        revision.timestamp = backy.utils.now()
        revision.write_info()
        backup.scan()

    assert rbdclient.snap_ls("test/foo") == [
        {'id': 86925,
        'name': 'someother',
        'protected': 'false',
        'size': 32212254720,
        'timestamp': 'Sun Feb 12 18:35:18 2023'},
        {'id': 86925,
        'name': 'backy-1',
        'protected': 'false',
        'size': 32212254720,
        'timestamp': 'Sun Feb 12 18:35:18 2023'},
    ]


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_choose_full_without_parent(check_output, backup, backend_factory):
    source = CephRBD(dict(pool="test", image="foo"))

    source.diff = mock.Mock()
    source.full = mock.Mock()

    revision = Revision(backup, "1")
    backend = backend_factory(revision)

    with source(revision):
        source.backup(backend)

    assert not source.diff.called
    assert source.full.called


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_choose_full_without_snapshot(check_output, backup, backend_factory):
    source = CephRBD(dict(pool="test", image="foo"))

    source.diff = mock.Mock()
    source.full = mock.Mock()

    revision1 = Revision(backup, "a1")
    revision1.timestamp = backy.utils.now()
    revision1.materialize()

    backup.scan()

    revision2 = Revision(backup, "a2")
    revision2.parent = "a1"

    backend = backend_factory(revision2)
    with source(revision2):
        source.backup(backend)

    assert not source.diff.called
    assert source.full.called


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_choose_diff_with_snapshot(
    check_output, backup, nosleep, backend_factory
):
    source = CephRBD(dict(pool="test", image="foo"))

    source.diff = mock.Mock()
    source.full = mock.Mock()

    revision1 = Revision(backup, "a1")
    revision1.timestamp = backy.utils.now()
    revision1.materialize()

    backup.scan()

    revision2 = Revision(backup, "a2")
    revision2.parent = "a1"

    check_output.side_effect = [
        # snap create
        b"{}",
        # snap ls
        b'[{"name": "backy-a1"}]',
        # snap ls
        b'[{"name": "backy-a1"}, {"name": "backy-a2"}]',
        # snap rm backy-a1
        b"{}",
    ]

    backend = backend_factory(revision2)
    with source(revision2):
        source.backup(backend)

    assert source.diff.called
    assert not source.full.called


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_diff_backup(check_output, backup, tmpdir, nosleep, backend_factory):
    from backy.sources.ceph.diff import RBDDiffV1

    source = CephRBD(dict(pool="test", image="foo"))

    parent = Revision(backup, "ed968696-5ab0-4fe0-af1c-14cadab44661")
    parent.timestamp = backy.utils.now()
    parent.materialize()

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision(backup, "f0e7292e-4ad8-4f2e-86d6-f40dca2aa802")
    revision.timestamp = backy.utils.now()
    revision.parent = parent.uuid

    with backend_factory(parent).open("wb") as f:
        f.write(b"asdf")

    backup.scan()
    revision.materialize()

    check_output.side_effect = [
        # snap create
        b"{}",
        # snap ls
        b'[{"name": "backy-ed968696-5ab0-4fe0-af1c-14cadab44661"}, \
           {"name": "backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802"}]',
        # snap rm backy-ed96...
        b"{}",
    ]

    with mock.patch("backy.sources.ceph.rbd.RBDClient.export_diff") as export:
        export.return_value = mock.MagicMock()
        export.return_value.__enter__.return_value = RBDDiffV1(
            io.BytesIO(SAMPLE_RBDDIFF)
        )
        backend = backend_factory(revision)
        with source(revision):
            parent = backup.find(revision.parent)
            source.diff(backend, parent)
            backup.history.append(revision)
        export.assert_called_with(
            "test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802",
            "backy-ed968696-5ab0-4fe0-af1c-14cadab44661",
        )

    assert check_output.call_args_list == [
        mock.call(
            [
                RBD,
                "snap",
                "create",
                "test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802",
            ],
            encoding='utf-8',
            errors='replace'
        ),
        mock.call([RBD, "snap", "ls", "test/foo" , "--format=json"],
            encoding='utf-8',
            errors='replace'
        ),
        mock.call(
            [
                RBD,
                "snap",
                "rm",
                "test/foo@backy-ed968696-5ab0-4fe0-af1c-14cadab44661",
            ],
            encoding='utf-8',
            errors='replace',
        ),
    ]


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_full_backup(check_output, backup, tmpdir, backend_factory):
    source = CephRBD(dict(pool="test", image="foo"))

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision(backup, "a0")
    revision.timestamp = backy.utils.now()
    revision.materialize()
    backup.scan()

    check_output.side_effect = [
        # snap create
        b"{}",
        # snap ls
        b'[{"name": "backy-a0"}]',
    ]

    with mock.patch("backy.sources.ceph.rbd.RBDClient.export") as export:
        export.return_value = io.BytesIO(b"Han likes Leia.")
        backend = backend_factory(revision)
        with source(revision):
            source.full(backend)
        export.assert_called_with("test/foo@backy-a0")

    assert check_output.call_args_list == [
        mock.call([RBD, "snap", "create", "test/foo@backy-a0"],
            encoding='utf-8',
            errors='replace'
        ),
        mock.call([RBD, "snap", "ls", "test/foo" , "--format=json"],
            encoding='utf-8',
            errors='replace'
        ),
    ]

    with backend.open("rb") as f:
        f.read() == b"Han likes Leia."

    # Now make another full backup. This overwrites the first.
    revision2 = Revision(backup, "a1")
    revision2.parent = revision.uuid
    revision2.materialize()
    backup.scan()

    check_output.side_effect = [
        # snap create
        b"{}",
        # snap ls
        b'[{"name": "backy-a1"}]',
    ]

    with mock.patch("backy.sources.ceph.rbd.RBDClient.export") as export:
        export.return_value = io.BytesIO(b"Han loves Leia.")
        backend = backend_factory(revision2)
        with source(revision2):
            source.full(backend)

    with backend.open("rb") as f:
        f.read() == b"Han loves Leia."


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_full_backup_integrates_changes(
    check_output, backup, tmpdir, nosleep, backend_factory
):
    # The backup source changes between two consecutive full backups. Both
    # backup images should reflect the state of the source at the time the
    # backup was run. This test is here to detect regressions while optimizing
    # the full backup algorithms (coping and applying deltas).
    source = CephRBD(dict(pool="test", image="foo"))
    content0 = BLOCK * b"A" + BLOCK * b"B" + BLOCK * b"C" + BLOCK * b"D"
    content1 = BLOCK * b"A" + BLOCK * b"X" + BLOCK * b"\0" + BLOCK * b"D"

    rev0 = Revision(backup, "a0")
    rev0.timestamp = backy.utils.now()
    rev0.materialize()
    backup.scan()

    rev1 = Revision(backup, "a1")
    rev1.parent = "a0"
    rev1.materialize()

    def returnvals(name):
        return [
            # snap create
            b"{}",
            # snap ls
            '[{{"name": "backy-{}"}}]'.format(name).encode("ascii"),
        ]

    check_output.side_effect = (
        returnvals("a0")
        + returnvals("a1")
        +
        # snap rm
        [b"{}"]
    )

    # check fidelity
    for content, rev in [(content0, rev0), (content1, rev1)]:
        with mock.patch("backy.sources.ceph.rbd.RBDClient.export") as export:
            export.return_value = io.BytesIO(content)
            backend = backend_factory(rev)
            with source(rev):
                source.full(backend)
            export.assert_called_with("test/foo@backy-{}".format(rev.uuid))

        with backend_factory(rev).open("rb") as f:
            assert content == f.read()


# --- mock data for tests involving RBD, uses data imported from rbd_testdata ---


def showmapped_json_legacy(device):
    return '{{"rbd0": {{"pool": "test", "name": "foo", "snap": "backy-a0", \
                    "device": "{}"}}}}'.format(
        device
    ).encode(
        "ascii"
    )


def showmapped_json_current(device):
    return '[{{"id": "0", "pool": "test", "namespace": "", "name": "foo", \
                    "snap": "backy-a0", \
                    "device": "{}"}}]'.format(
        device
    ).encode(
        "ascii"
    )


# ---


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_verify_fail(
    check_output,
    backup,
    tmpdir,
    backend_factory,
    rbdclient
):
    source = CephRBD(dict(pool="test", image="foo"))
    source.rbd = rbdclient
    rbdclient._ceph_cli._register_image_for_snaps("test/foo")

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision(backup, "a0")
    revision.timestamp = backy.utils.now()
    revision.materialize()

    backup.scan()

    rbd_source = str(tmpdir / "-dev-rbd0")
    with open(rbd_source, "w") as f:
        f.write("Han likes Leia.")

    backend = backend_factory(revision)
    with backend.open("wb") as f:
        f.write(b"foobar")
    # The backend has false data, so this needs to be detected.
    with source(revision):
        assert not source.verify(backend)


@pytest.mark.parametrize(
    "backend_factory", [COWFileBackend, ChunkedFileBackend]
)
def test_verify(
    rbdclient,
    backup,
    tmpdir,
    backend_factory,
):
    source = CephRBD(dict(pool="test", image="foo"))
    source.rbd = rbdclient
    rbdclient._ceph_cli._register_image_for_snaps("test/foo")

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision(backup, "a0")
    revision.timestamp = backy.utils.now()
    revision.materialize()

    backup.scan()

    rbd_source = rbdclient.map("test/foo@backy-a0")["device"]
    with open(rbd_source, "wb") as f:
        f.write(b"Han likes Leia.")
    rbdclient.unmap(rbd_source)

    with backend_factory(revision).open("wb") as f:
        f.write(b"Han likes Leia.")
        f.flush()

    backend = backend_factory(revision)
    with source(revision):
        assert source.verify(backend)


def test_ceph_config_from_cli():
    s = CephRBD.config_from_cli("rbd/vm.root")
    assert s == {
        "pool": "rbd",
        "image": "vm.root",
    }


def test_ceph_config_from_cli_invalid():
    with pytest.raises(RuntimeError) as exc:
        CephRBD.config_from_cli("foobar")
    assert str(exc.value) == (
        "ceph source must be initialized with " "POOL/IMAGE"
    )
