import os
import subprocess
from unittest import mock

import pytest

import backy.sources.ceph
from backy.ext_deps import RBD
from backy.sources.ceph.diff import RBDDiffV1
from backy.sources.ceph.rbd import RBDClient


@mock.patch("subprocess.check_output")
def test_rbd_command_wrapper(check_output, log):
    client = RBDClient(log)

    check_output.return_value = ""
    client._rbd(["foo"])
    check_output.assert_called_with(
        [RBD, "foo"], encoding="utf-8", errors="replace"
    )

    check_output.return_value = '{"asdf": 1}'
    result = client._rbd(["foo"], format="json")
    assert result == {"asdf": 1}
    check_output.assert_called_with(
        [RBD, "foo", "--format=json"], encoding="utf-8", errors="replace"
    )


def test_rbd_generic_calledprocesserror_bubbles_up(rbdclient):
    # Generic errors are bubbled up,
    # explicitly check the subprocess backend
    rbdclient._ceph_cli = mock.Mock()
    rbdclient._ceph_cli.side_effect = subprocess.CalledProcessError(
        returncode=1, cmd="foo"
    )
    with pytest.raises(subprocess.CalledProcessError):
        rbdclient.exists("test/asdf")


def test_rbd_nonexisting_image_turned_to_false(rbdclient):
    assert not rbdclient.exists("test/foo")


# uses mock data defined in conftest.py


def test_rbd_map_unmap_writable(rbdclient):
    mapped = rbdclient.map("test/test04.root@backup")
    # device is a tempfile, needs to be checked separately
    map_dev = mapped.pop("device")
    assert map_dev.endswith("/rbd0")
    assert mapped == {
        "name": "test04.root",
        "pool": "test",
        "snap": "backup",
    }
    rbdclient.unmap(map_dev)


def test_rbd_map_readonly(rbdclient):
    mapped = rbdclient.map("test/test04.root@backup", readonly=True)
    # device is a tempfile, needs to be checked separately
    assert mapped.pop("device").endswith("/rbd0")
    assert mapped == {
        "name": "test04.root",
        "pool": "test",
        "snap": "backup",
    }


def test_rbd_map_writable_missing_map_no_maps(rbdclient):
    # enforce empty showmapped
    rbdclient._ceph_cli._freeze_mapped = True
    with pytest.raises(RuntimeError):
        rbdclient.map("test/test04.root@backup", readonly=True)


def test_rbd_map_writable_missing_map(rbdclient):
    # enforce empty showmapped
    rbdclient._ceph_cli._freeze_mapped = True
    with pytest.raises(RuntimeError):
        rbdclient.map("test/test04.root@backup", readonly=True)


def test_rbd_snap(rbdclient):
    # test setup: mark image as existing, as rbd snap only operates on existing images
    rbdclient._ceph_cli._register_image_for_snaps("test/test04.root")
    rbdclient.snap_create("test/test04.root@backup-asdf")
    assert rbdclient.snap_ls("test/test04.root")[0]["name"] == "backup-asdf"
    with pytest.raises(subprocess.CalledProcessError):
        rbdclient.snap_rm("test/test04.root@backup-nonasdf")
    with pytest.raises(subprocess.CalledProcessError):
        rbdclient.snap_rm("test/test42.root@backup-asdf")
    rbdclient.snap_rm("test/test04.root@backup-asdf")
    assert len(rbdclient.snap_ls("test/test04.root")) == 0


@mock.patch("subprocess.Popen")
def test_rbd_export_diff(popen, rbdclient, tmpdir):
    stdout = open(str(tmpdir / "foobar"), "wb+")
    stdout.write(RBDDiffV1.header)
    stdout.seek(0)
    popen.return_value = mock.Mock(stdout=stdout)
    with rbdclient.export_diff("test/test04.root@new", "old") as diff:
        assert isinstance(diff, RBDDiffV1)
    popen.assert_has_calls(
        [
            mock.call(
                [
                    RBD,
                    "export-diff",
                    "test/test04.root@new",
                    "--from-snap",
                    "old",
                    "--whole-object",
                    "-",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                bufsize=mock.ANY,
            )
        ]
    )


def test_rbd_image_reader_simple(rbdclient, tmpdir):
    device = rbdclient.map("test/test04.root@backup")["device"]
    open(device, "wb").write(b"asdf")
    with rbdclient.image_reader("test/test04.root@backup") as f:
        assert f.name == device


def test_rbd_image_reader_explicit_closed(rbdclient, tmpdir):
    device = rbdclient.map("test/test04.root@backup")["device"]
    open(device, "wb").write(b"asdf")

    with rbdclient.image_reader("test/test04.root@backup") as f:
        assert f.read() == b"asdf"
        f.close()


@mock.patch("subprocess.Popen")
def test_rbd_export(popen, rbdclient, tmpdir):
    stdout = open(str(tmpdir / "rbd0"), "wb+")
    popen.return_value = mock.Mock(stdout=stdout)
    with rbdclient.export(mock.sentinel.image) as f:
        assert f == stdout
    popen.assert_has_calls(
        [
            mock.call(
                [RBD, "export", mock.sentinel.image, "-"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                bufsize=mock.ANY,
            ),
        ]
    )
