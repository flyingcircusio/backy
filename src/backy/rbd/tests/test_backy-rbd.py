import os
import subprocess

import pytest

from backy.ext_deps import BACKY_RBD_CMD, BASH
from backy.rbd import RbdSource
from backy.rbd.conftest import create_rev
from backy.revision import Revision
from backy.tests import Ellipsis


def generate_test_data(target, size, marker):
    f = open(target, "wb")
    block = 8 * 1024
    for chunk in range(size // block):
        f.write(marker * block)
    f.write(marker * (size % block))
    f.close()


def test_smoketest_internal(tmp_path, log):
    # These copies of data are intended to be different versions of the same
    # file.
    source1 = str(tmp_path / "image1.qemu")
    generate_test_data(source1, 2 * 1024**2, b"1")
    source2 = str(tmp_path / "image2.qemu")
    generate_test_data(source2, 2 * 1024**2, b"2")
    source3 = str(tmp_path / "image3.qemu")
    generate_test_data(source3, 2 * 1024**2, b"3")

    backup_dir = tmp_path / "image1.backup"
    os.mkdir(str(backup_dir))
    with open(str(backup_dir / "config"), "wb") as f:
        f.write(
            (
                "{'source': {'type': 'file', 'filename': '%s'},"
                "'schedule': {'daily': {'interval': '1d', 'keep': 7}}}"
                % source1
            ).encode("utf-8")
        )
    repository = RbdSource(backup_dir, log)

    # Backup first state
    rev1 = create_rev(repository, {"manual:test"})
    repository.backup(rev1.uuid)

    # Restore first state from the newest revision
    restore_target = str(tmp_path / "image1.restore")
    repository.restore(rev1.uuid, restore_target)
    with pytest.raises(IOError):
        open(repository.history[-1].filename, "wb")
    with pytest.raises(IOError):
        open(repository.history[-1].info_filename, "wb")
    assert open(source1, "rb").read() == open(restore_target, "rb").read()

    # Backup second state
    repository.source.filename = source2
    rev2 = create_rev(repository, {"test"})
    repository.backup(rev2.uuid)
    assert len(repository.history) == 2

    # Restore second state from second backup which is the newest at position 0
    repository.restore(rev2.uuid, restore_target)
    d1 = open(source2, "rb").read()
    d2 = open(restore_target, "rb").read()
    assert d1 == d2

    # Our original backup is now at position 1. Lets restore that again.
    repository.restore(rev1.uuid, restore_target)
    assert open(source1, "rb").read() == open(restore_target, "rb").read()

    # Backup second state again
    repository.source.filename = source2
    rev3 = create_rev(repository, {"manual:test"})
    repository.backup(rev3.uuid)
    assert len(repository.history) == 3

    # Restore image2 from its most recent at position 0
    repository.restore(rev3.uuid, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    # Restore image2 from its previous backup, now at position 1
    repository.restore(rev2.uuid, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    # Our original backup is now at position 2. Lets restore that again.
    repository.restore(rev1.uuid, restore_target)
    assert open(source1, "rb").read() == open(restore_target, "rb").read()

    # Backup third state
    repository.source.filename = source3
    rev4 = create_rev(repository, {"test"})
    repository.backup(rev4.uuid)
    assert len(repository.history) == 4

    # Restore image3 from the most curent state
    repository.restore(rev4.uuid, restore_target)
    assert open(source3, "rb").read() == open(restore_target, "rb").read()

    # Restore image2 from position 1 and 2
    repository.restore(rev3.uuid, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    repository.restore(rev2.uuid, restore_target)
    assert open(source2, "rb").read() == open(restore_target, "rb").read()

    # Restore image1 from position 3
    repository.restore(rev1.uuid, restore_target)
    assert open(source1, "rb").read() == open(restore_target, "rb").read()


@pytest.mark.slow
def test_smoketest_external():
    output = subprocess.check_output(
        [BASH, os.path.dirname(__file__) + "/smoketest.sh"],
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
