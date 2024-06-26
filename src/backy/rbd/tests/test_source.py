from backy.rbd import RbdRepository
from backy.rbd.sources.ceph.source import CephRBD


def test_configure_ceph_source(tmp_path, log):
    with open(str(tmp_path / "config"), "w") as f:
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
    backup = RbdRepository(tmp_path, log)
    assert isinstance(backup.source, CephRBD)
    assert backup.source.pool == "test"
    assert backup.source.image == "test04"
