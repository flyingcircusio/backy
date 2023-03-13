from backy.backup import Backup
from backy.sources.ceph.source import CephRBD


def test_configure_ceph_source(tmpdir, log):
    with open(str(tmpdir / "config"), "w") as f:
        f.write(
            """\
---
    schedule:
        daily:
            interval: 1d
            keep: 7
    source:
        type: ceph-rbd
        pool: test
        image: test04
"""
        )
    backup = Backup(str(tmpdir), log)
    assert isinstance(backup.source, CephRBD)
    assert backup.source.pool == "test"
    assert backup.source.image == "test04"
