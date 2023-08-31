import os
import subprocess

import pytest

import backy.backup
from backy.ext_deps import BACKY_CMD, BASH
from backy.tests import Ellipsis


def generate_test_data(target, size, marker):
    f = open(target, "wb")
    block = 8 * 1024
    for chunk in range(size // block):
        f.write(marker * block)
    f.write(marker * (size % block))
    f.close()


def test_smoketest_internal(tmpdir, log):
    # These copies of data are intended to be different versions of the same
    # file.
    source1 = str(tmpdir / "image1.qemu")
    generate_test_data(source1, 2 * 1024**2, b"1")
    source2 = str(tmpdir / "image2.qemu")
    generate_test_data(source2, 2 * 1024**2, b"2")
    source3 = str(tmpdir / "image3.qemu")
    generate_test_data(source3, 2 * 1024**2, b"3")

    backup_dir = tmpdir / "image1.backup"
    os.mkdir(str(backup_dir))
    with open(str(backup_dir / "config"), "wb") as f:
        f.write(
            (
                "{'source': {'type': 'file', 'filename': '%s'},"
                "'schedule': {'daily': {'interval': '1d', 'keep': 7}}}"
                % source1
            ).encode("utf-8")
        )
    backup = backy.backup.Backup(str(backup_dir), log)

    # Backup first state
    backup.backup({"manual:test"})

    # Restore first state form newest revision at position 0
    restore_target = str(tmpdir / "image1.restore")
    backup.restore(0, restore_target)
    with pytest.raises(IOError):
        open(backup.history[-1].filename, "wb")
    with pytest.raises(IOError):
        open(backup.history[-1].info_filename, "wb")
    assert open(source1, "rb").read() == open(restore_target, "rb").read()

    # Backup second state
    backup.source.filename = source2
    backup.backup({"test"}, force=True)
    assert len(backup.history) == 2

    # Restore second state from second backup which is the newest at position 0
    backup.restore(0, restore_target)
    d1 = open(source2, "rb").read()
    d2 = open(restore_target, "rb").read()
    assert d1 == d2

    # Our original backup is now at position 1. Lets restore that again.
    backup.restore(1, restore_target)
    assert open(source1, "rb").read() == open(restore_target, "rb").read()

    # Backup second state again
    backup.source.filename = source2
    backup.backup({"manual:test"})
    assert len(backup.history) == 3

    # Restore image2 from its most recent at position 0
    backup.restore(0, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    # Restore image2 from its previous backup, now at position 1
    backup.restore(1, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    # Our original backup is now at position 2. Lets restore that again.
    backup.restore(2, restore_target)
    assert open(source1, "rb").read() == open(restore_target, "rb").read()

    # Backup third state
    backup.source.filename = source3
    backup.backup({"test"}, True)
    assert len(backup.history) == 4

    # Restore image3 from the most curent state
    backup.restore(0, restore_target)
    assert open(source3, "rb").read() == open(restore_target, "rb").read()

    # Restore image2 from position 1 and 2
    backup.restore(1, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    backup.restore(2, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    # Restore image1 from position 3
    backup.restore(3, restore_target)
    assert open(source1, "rb").read() == open(restore_target, "rb").read()


@pytest.mark.slow
def test_smoketest_external():
    output = subprocess.check_output(
        [BASH, os.path.dirname(__file__) + "/../../../smoketest.sh"],
        env=os.environ | {"BACKY_CMD": BACKY_CMD},
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
+----------------------+------------------------+-----------+----------+-------------+---------+
| Date (...) | ID                     |      Size | Duration | Tags        | Trust   |
+----------------------+------------------------+-----------+----------+-------------+---------+
| ...  | ... | 512.0 KiB | a moment | manual:test | trusted |
| ...  | ... | 512.0 KiB | a moment | daily       | trusted |
| ...  | ... | 512.0 KiB | a moment | test        | trusted |
| ...  | ... | 512.0 KiB | a moment | manual:test | trusted |
+----------------------+------------------------+-----------+----------+-------------+---------+
4 revisions containing 2.0 MiB data (estimated)
+----------------------+------------------------+-----------+----------+-------------+---------+
| Date (...) | ID                     |      Size | Duration | Tags        | Trust   |
+----------------------+------------------------+-----------+----------+-------------+---------+
| ...  | ... | 512.0 KiB | a moment | manual:test | trusted |
| ...  | ... | 512.0 KiB | a moment | test        | trusted |
| ...  | ... | 512.0 KiB | a moment | manual:test | trusted |
+----------------------+------------------------+-----------+----------+-------------+---------+
3 revisions containing 1.5 MiB data (estimated)
"""
        )
        == output
    )
