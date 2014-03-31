import backy.backup
import os
import pytest
from backy.tests import Ellipsis
import subprocess


def generate_test_data(target, size):
    f = open(target, 'wb')
    block = 8*1024
    for chunk in range(size // block):
        f.write(os.urandom(block))
    f.write(os.urandom(size % block))
    f.close()


@pytest.mark.smoke
def test_smoketest_internal(tmpdir):
    # These copies of data are intended to be different versions of the same
    # file.
    source1 = str(tmpdir / 'image1.qemu')
    generate_test_data(source1, 20*1024**2)
    source2 = str(tmpdir / 'image2.qemu')
    generate_test_data(source2, 20*1024**2)
    source3 = str(tmpdir / 'image3.qemu')
    generate_test_data(source3, 20*1024**2)

    backup_dir = str(tmpdir / 'image1.backup')
    os.mkdir(backup_dir)
    backup = backy.backup.Backup(backup_dir)
    backup.init(source1)

    # Backup first state
    backup.backup(source1)

    # Restore first state from level 0
    restore_target = str(tmpdir / 'image1.restore')
    backup.restore(restore_target, 0)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()

    # Backup second state
    backup.backup(source2)

    # Restore second state from level 0
    backup.restore(restore_target, 0)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Our original backup has now become level 1. Lets restore that again.
    backup.restore(restore_target, 1)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()

    # Backup second state again
    backup.backup(source2)

    # Restore image2 from level 0 again
    backup.restore(restore_target, 0)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Restore image2 from level 1
    backup.restore(restore_target, 1)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Our original backup has now become level 2. Lets restore that again.
    backup.restore(restore_target, 2)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()

    # Backup third state
    backup.backup(source3)

    # Restore image3 from level 0
    backup.restore(restore_target, 0)
    assert open(source3, 'r').read() == open(restore_target, 'r').read()

    # Restore image2 from level 1
    backup.restore(restore_target, 1)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Restore image2 from level 2
    backup.restore(restore_target, 2)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Restore image1 from level 3
    backup.restore(restore_target, 3)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()


@pytest.mark.smoke
def test_smoketest_external():
    output = subprocess.check_output(
        os.path.dirname(__file__) + '/../../../smoketest.sh',
        shell=True)
    assert Ellipsis("""\
Using ... as workspace.
Generating Test Data.. Done.
Backing up img_state1.img. Starting to back up revision ...
000000 | ... | READ
000000 | ... | PROCESS
000000 | ... | STORE MAIN
000001 | ... | READ
000001 | ... | PROCESS
000001 | ... | STORE MAIN
000002 | ... | READ
000002 | ... | PROCESS
000002 | ... | STORE MAIN
000003 | ... | READ
000003 | ... | PROCESS
000003 | ... | STORE MAIN
000004 | ... | READ
000004 | ... | PROCESS
000004 | ... | STORE MAIN
Done.
Restoring img_state1.img from level 0. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state2.img. Starting to back up revision ...
     using delta revision ...
000000 | ... | READ
000000 | ... | PROCESS
000000 | ... | STORE DELTA
000000 | ... | STORE MAIN
000001 | ... | READ
000001 | ... | PROCESS
000001 | ... | STORE DELTA
000001 | ... | STORE MAIN
000002 | ... | READ
000002 | ... | PROCESS
000002 | ... | STORE DELTA
000002 | ... | STORE MAIN
000003 | ... | READ
000003 | ... | PROCESS
000003 | ... | STORE DELTA
000003 | ... | STORE MAIN
000004 | ... | READ
000004 | ... | PROCESS
000004 | ... | STORE DELTA
000004 | ... | STORE MAIN
Done.
Restoring img_state2.img from level 0. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 1. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state2.img again. Starting to back up revision ...
     using delta revision ...
000000 | ... | READ
000000 | ... | PROCESS
000001 | ... | READ
000001 | ... | PROCESS
000002 | ... | READ
000002 | ... | PROCESS
000003 | ... | READ
000003 | ... | PROCESS
000004 | ... | READ
000004 | ... | PROCESS
Done.
Restoring img_state2.img from level 0. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state2.img from level 1. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 2. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state3.img. Starting to back up revision ...
     using delta revision ...
000000 | ... | READ
000000 | ... | PROCESS
000000 | ... | STORE DELTA
000000 | ... | STORE MAIN
000001 | ... | READ
000001 | ... | PROCESS
000001 | ... | STORE DELTA
000001 | ... | STORE MAIN
000002 | ... | READ
000002 | ... | PROCESS
000002 | ... | STORE DELTA
000002 | ... | STORE MAIN
000003 | ... | READ
000003 | ... | PROCESS
000003 | ... | STORE DELTA
000003 | ... | STORE MAIN
000004 | ... | READ
000004 | ... | PROCESS
000004 | ... | STORE DELTA
000004 | ... | STORE MAIN
Done.
Restoring img_state3.img from level 0. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state3.img against img_state3.img. Success.
Restoring img_state2.img from level 1. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state2.img from level 2. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 3. Restoring revision ...
000000 | - | RESTORE
000001 | - | RESTORE
000002 | - | RESTORE
000003 | - | RESTORE
000004 | - | RESTORE
Restored with matching checksum.
Done.
Diffing restore_state1.img against img_state1.img. Success.
Scrubbing ...
OK
Scrubbing ...
OK
Scrubbing ...
OK
Scrubbing ...
OK
== Revisions
... 5   ...
... 0   ...
... 5   ...
... 5   ...

== Summary
4 revisions with 15 blocks (~3 blocks/revision)
Removing revision ...
Removing revision ...
Removing revision ...
== Revisions
... 5   ...

== Summary
1 revisions with 5 blocks (~5 blocks/revision)
""") == output
