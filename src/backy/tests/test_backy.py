from backy.ext_deps import BASH
from backy.tests import Ellipsis
import backy.backup
import os
import pytest
import subprocess


def generate_test_data(target, size):
    f = open(target, 'wb')
    block = 8 * 1024
    for chunk in range(size // block):
        f.write(os.urandom(block))
    f.write(os.urandom(size % block))
    f.close()


def test_smoketest_internal(tmpdir, forget_about_btrfs):
    # These copies of data are intended to be different versions of the same
    # file.
    source1 = str(tmpdir / 'image1.qemu')
    generate_test_data(source1, 2 * 1024 ** 2)
    source2 = str(tmpdir / 'image2.qemu')
    generate_test_data(source2, 2 * 1024 ** 2)
    source3 = str(tmpdir / 'image3.qemu')
    generate_test_data(source3, 2 * 1024 ** 2)

    backup_dir = str(tmpdir / 'image1.backup')
    os.mkdir(backup_dir)
    backup = backy.backup.Backup(backup_dir)
    backup.init('file', source1)

    # Backup first state
    backup.configure()
    backup.source.filename = source1
    backup.backup(tags='test')

    # Restore first state from level 0
    restore_target = str(tmpdir / 'image1.restore')
    backup.restore(0, restore_target)
    with pytest.raises(IOError):
        open(backup.archive.history[-1].filename, 'wb')
    with pytest.raises(IOError):
        open(backup.archive.history[-1].info_filename, 'wb')
    assert open(source1, 'rb').read() == open(restore_target, 'rb').read()

    # Backup second state
    backup.source.filename = source2
    backup.backup(tags='test')
    assert len(backup.archive.history) == 2

    # Restore second state from level 0
    backup.restore(0, restore_target)
    assert open(source2, 'rb').read() == open(restore_target, 'rb').read()

    # Our original backup has now become level 1. Lets restore that again.
    backup.restore(1, restore_target)
    assert open(source1, 'rb').read() == open(restore_target, 'rb').read()

    # Backup second state again
    backup.source.filename = source2
    backup.backup(tags='test')
    assert len(backup.archive.history) == 3

    # Restore image2 from level 0 again
    backup.restore(0, restore_target)
    assert open(source2, 'rb').read() == open(restore_target, 'rb').read()

    # Restore image2 from level 1
    backup.restore(1, restore_target)
    assert open(source2, 'rb').read() == open(restore_target, 'rb').read()

    # Our original backup has now become level 2. Lets restore that again.
    backup.restore(2, restore_target)
    assert open(source1, 'rb').read() == open(restore_target, 'rb').read()

    # Backup third state
    backup.source.filename = source3
    backup.backup(tags='test')
    assert len(backup.archive.history) == 4

    # Restore image3 from level 0
    backup.restore(0, restore_target)
    assert open(source3, 'rb').read() == open(restore_target, 'rb').read()

    # Restore image2 from level 1
    backup.restore(1, restore_target)
    assert open(source2, 'rb').read() == open(restore_target, 'rb').read()

    # Restore image2 from level 2
    backup.restore(2, restore_target)
    assert open(source2, 'rb').read() == open(restore_target, 'rb').read()

    # Restore image1 from level 3
    backup.restore(3, restore_target)
    assert open(source1, 'rb').read() == open(restore_target, 'rb').read()


@pytest.mark.slow
def test_smoketest_external(forget_about_btrfs):
    output = subprocess.check_output([
        BASH, os.path.dirname(__file__) + '/../../../smoketest.sh'])
    output = output.decode('utf-8')
    assert Ellipsis("""\
Using /... as workspace.
Generating Test Data.. Done.
Backing up img_state1.img. Done.
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
+-------------------------+------------------------+------------+----------+------+
| Date                    | ID                     |       Size | Duration | \
Tags |
+-------------------------+------------------------+------------+----------+------+
| ... | ... | 511.99 kiB |        0 | test |
| ... | ... | 511.99 kiB |        0 | test |
| ... | ... | 511.99 kiB |        0 | test |
| ... | ... | 511.99 kiB |        0 | test |
+-------------------------+------------------------+------------+----------+------+
== Summary
4 revisions
2.00 MiB data (estimated)
""") == output
