import os
import backy
import backy.operations
import pytest


def generate_test_data(target, size):
    f = open(target, 'wb')
    for chunk in range(size // backy.CHUNKSIZE):
        f.write(os.urandom(backy.CHUNKSIZE))
    f.write(os.urandom(size % backy.CHUNKSIZE))
    f.close()


@pytest.mark.smoke
def test_comprehensive_workflow(tmpdir):
    # These copies of data are intended to be different versions of the same
    # file.
    source1 = str(tmpdir / 'image1.qemu')
    generate_test_data(source1, 20*1024**2)
    source2 = str(tmpdir / 'image2.qemu')
    generate_test_data(source2, 20*1024**2)
    source3 = str(tmpdir / 'image3.qemu')
    generate_test_data(source3, 20*1024**2)

    backup = str(tmpdir / 'image1.backup')
    os.mkdir(backup)

    # Backup first state
    backy.operations.backup(source1, backup)

    # Restore first state from level 0
    restore_target = str(tmpdir / 'image1.restore')
    backy.operations.restore(backup, restore_target, 0)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()

    # Backup second state
    backy.operations.backup(source2, backup)

    # Restore second state from level 0
    backy.operations.restore(backup, restore_target, 0)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Our original backup has now become level 1. Lets restore that again.
    backy.operations.restore(backup, restore_target, 1)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()

    # Backup second state again
    backy.operations.backup(source2, backup)

    # Restore image2 from level 0 again
    backy.operations.restore(backup, restore_target, 0)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Restore image2 from level 1
    backy.operations.restore(backup, restore_target, 1)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Our original backup has now become level 2. Lets restore that again.
    backy.operations.restore(backup, restore_target, 2)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()

    # Backup third state
    backy.operations.backup(source3, backup)

    # Restore image3 from level 0
    backy.operations.restore(backup, restore_target, 0)
    assert open(source3, 'r').read() == open(restore_target, 'r').read()

    # Restore image2 from level 1
    backy.operations.restore(backup, restore_target, 1)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Restore image2 from level 2
    backy.operations.restore(backup, restore_target, 2)
    assert open(source2, 'r').read() == open(restore_target, 'r').read()

    # Restore image1 from level 3
    backy.operations.restore(backup, restore_target, 3)
    assert open(source1, 'r').read() == open(restore_target, 'r').read()
