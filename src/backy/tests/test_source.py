from backy.sources.ceph.source import CephRBD
from backy.backup import Backup


def test_configure_ceph_source(tmpdir):
    with open(str(tmpdir / 'config'), "w") as f:
        f.write("""\
{"source": {"pool": "test", "image": "test04"}, "chunksize": 4096, \
"source-type": "ceph-rbd"}\
""")
    backup = Backup(str(tmpdir))
    assert isinstance(backup.source, CephRBD)
    assert backup.source.pool == 'test'
    assert backup.source.image == 'test04'
