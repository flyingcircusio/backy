from backy.source import CephSource
from backy.backup import Backup
from mock import Mock, call, patch


@patch("backy.source.cmd")
@patch("backy.source.Source.open")
@patch("backy.source.Source.close")
def test_ceph_source(close, open, cmd):
    source = CephSource('test/test04', Mock())
    source.open()
    cmd.return_value = """\
{"1": {"pool": "test", "name": "test04", "snap": "backy", \
"device": "/dev/rbd1"}}\
"""
    source.close()
    cmd.assert_has_calls([
        call.cmd('rbd snap create test/test04@backy', shell=True).
        call.cmd('rbd map test/test04@backy', shell=True).
        call.cmd('rbd --format=json showmapped', shell=True).
        call.cmd('rbd unmap /dev/rbd1', shell=True).
        call.cmd('rbd snap rm test/test04@backy', shell=True)])


def test_configure_ceph_source(tmpdir):
    with open(str(tmpdir / 'config'), "w") as f:
        f.write("""\
{"source": "test/test04", "chunksize": 4096, "source-type": "ceph-rbd"}\
""")
    backup = Backup(str(tmpdir))
    assert isinstance(backup.source, CephSource)
    assert backup.source.filename == 'test/test04'