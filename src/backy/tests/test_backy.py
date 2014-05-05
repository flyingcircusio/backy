from backy.tests import Ellipsis
import backy.backup
import os
import subprocess
import time


def generate_test_data(target, size):
    f = open(target, 'wb')
    block = 8*1024
    for chunk in range(size // block):
        f.write(os.urandom(block))
    f.write(os.urandom(size % block))
    f.close()


def test_smoketest_internal(tmpdir):
    # These copies of data are intended to be different versions of the same
    # file.
    source1 = str(tmpdir / 'image1.qemu')
    generate_test_data(source1, 2*1024**2)
    source2 = str(tmpdir / 'image2.qemu')
    generate_test_data(source2, 2*1024**2)
    source3 = str(tmpdir / 'image3.qemu')
    generate_test_data(source3, 2*1024**2)

    backup_dir = str(tmpdir / 'image1.backup')
    os.mkdir(backup_dir)
    backup = backy.backup.Backup(backup_dir)
    backup.init('file', source1)

    # Backup first state
    backup.source.filename = source1
    backup.backup()

    # Restore first state from level 0
    restore_target = str(tmpdir / 'image1.restore')
    backup.restore(0, restore_target)
    assert open(source1, 'rb').read() == open(restore_target, 'rb').read()

    # Backup second state
    backup.source.filename = source2
    backup.backup()

    # Assert that nothing happened: 24 hours interval by default
    backup._scan_revisions()
    assert len(backup.revision_history) == 1

    # Set interval to 0 to let tests continue
    backup.INTERVAL = 0
    time.sleep(1)
    backup.backup()

    # Restore second state from level 0
    backup.restore(0, restore_target)
    assert open(source2, 'rb').read() == open(restore_target, 'rb').read()

    # Our original backup has now become level 1. Lets restore that again.
    backup.restore(1, restore_target)
    assert open(source1, 'rb').read() == open(restore_target, 'rb').read()

    # Backup second state again
    time.sleep(1)
    backup.source.filename = source2
    backup.backup()

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
    backup.backup()

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


def test_smoketest_external():
    output = subprocess.check_output(
        os.path.dirname(__file__) + '/../../../smoketest.sh',
        shell=True)
    output = output.decode('utf-8')
    assert Ellipsis("""\
Using /... as workspace.
Generating Test Data.. Done.
Backing up img_state1.img. Done.
Restoring img_state1.img from level 0. Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state2.img. Backup due since .... Starting.
Performing non-COW copy: ...
Done.
Restoring img_state2.img from level 0. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 1. Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state2.img again. Backup due since .... Starting.
Performing non-COW copy: ...
Done.
Restoring img_state2.img from level 0. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state2.img from level 1. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 2. Done.
Diffing restore_state1.img against img_state1.img. Success.
Backing up img_state3.img. Backup due since .... Starting.
Performing non-COW copy: ...
Done.
Restoring img_state3.img from level 0. Done.
Diffing restore_state3.img against img_state3.img. Success.
Restoring img_state2.img from level 1. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state2.img from level 2. Done.
Diffing restore_state2.img against img_state2.img. Success.
Restoring img_state1.img from level 3. Done.
Diffing restore_state1.img against img_state1.img. Success.
== Revisions
... 5.00 MiB        0s
... 5.00 MiB        0s
... 5.00 MiB        0s
... 5.00 MiB        0s

== Summary
4 revisions
20.00 MiB data (estimated)
Removing revision ...
Removing revision ...
Removing revision ...
== Revisions
... 5.00 MiB ... 0s

== Summary
1 revisions
5.00 MiB data (estimated)
""") == output
