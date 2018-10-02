from backy.ext_deps import RBD
from backy.sources.ceph.diff import RBDDiffV1
from backy.sources.ceph.rbd import RBDClient
from unittest import mock
import pytest
import subprocess


@mock.patch('subprocess.check_output')
def test_rbd_command_wrapper(check_output):
    client = RBDClient()

    client._rbd(['foo'])
    check_output.assert_called_with([RBD, 'foo'])

    check_output.return_value = b'{"asdf": 1}'
    result = client._rbd(['foo'], format='json')
    assert result == {'asdf': 1}
    check_output.assert_called_with([RBD, '--format=json', 'foo'])


@pytest.fixture
def rbdclient():
    client = RBDClient()
    client._rbd = mock.Mock()
    return client


def test_rbd_exists(rbdclient):
    rbdclient._rbd.return_value = ''
    assert not rbdclient.exists('asdf')
    rbdclient._rbd.assert_called_with(['info', 'asdf'], format='json')


def test_rbd_generic_calledprocesserror_bubbles_up(rbdclient):
    # Generic errors are bubbled up
    rbdclient._rbd.side_effect = subprocess.CalledProcessError(
        returncode=1, cmd='foo')
    with pytest.raises(subprocess.CalledProcessError):
        rbdclient.exists('asdf')


def test_rbd_nonexisting_image_turned_to_false(rbdclient):
    rbdclient._rbd.side_effect = subprocess.CalledProcessError(
        returncode=2, cmd='foo')
    assert not rbdclient.exists('foo')
    rbdclient._rbd.assert_called_with(['info', 'foo'], format='json')


def test_rbd_map_writable(rbdclient):
    rbdclient._rbd.side_effect = [
        None,
        {'1': {'pool': 'test', 'name': 'test04.root', 'snap': 'backup'}}]
    mapped = rbdclient.map('test/test04.root@backup')
    assert mapped == {'pool': 'test', 'name': 'test04.root', 'snap': 'backup'}
    rbdclient._rbd.assert_has_calls([
        mock.call(['', 'map', 'test/test04.root@backup']),
        mock.call(['showmapped'], format='json')])


def test_rbd_map_readonly(rbdclient):
    rbdclient._rbd.side_effect = [
        None,
        {'1': {'pool': 'test', 'name': 'test04.root', 'snap': 'backup'}}]
    mapped = rbdclient.map('test/test04.root@backup', readonly=True)
    assert mapped == {'pool': 'test', 'name': 'test04.root', 'snap': 'backup'}
    rbdclient._rbd.assert_has_calls([
        mock.call(['--read-only', 'map', 'test/test04.root@backup']),
        mock.call(['showmapped'], format='json')])


def test_rbd_map_writable_missing_map_no_maps(rbdclient):
    rbdclient._rbd.side_effect = [None, {}]
    with pytest.raises(RuntimeError):
        rbdclient.map('test/test04.root@backup', readonly=True)
    rbdclient._rbd.assert_has_calls([
        mock.call(['--read-only', 'map', 'test/test04.root@backup']),
        mock.call(['showmapped'], format='json')])


def test_rbd_map_writable_missing_map(rbdclient):
    rbdclient._rbd.side_effect = [
        None,
        {'sadf': {'pool': 'sadf', 'name': 'asdf', 'snap': 'asdf'}}]
    with pytest.raises(RuntimeError):
        rbdclient.map('test/test04.root@backup', readonly=True)
    rbdclient._rbd.assert_has_calls([
        mock.call(['--read-only', 'map', 'test/test04.root@backup']),
        mock.call(['showmapped'], format='json')])


def test_rbd_unmap(rbdclient):
    rbdclient.unmap('asdf')
    rbdclient._rbd.assert_has_calls([
        mock.call(['unmap', 'asdf'])])


def test_rbd_snap_create(rbdclient):
    rbdclient.snap_create('test/test04.root@backup')
    rbdclient._rbd.assert_has_calls([
        mock.call(['snap', 'create', 'test/test04.root@backup'])])


def test_rbd_snap_ls(rbdclient):
    rbdclient._rbd.return_value = 'asdf'
    ls = rbdclient.snap_ls('test/test04.root')
    assert ls == 'asdf'
    rbdclient._rbd.assert_has_calls([
        mock.call(['snap', 'ls', 'test/test04.root'], format='json')])


def test_rbd_snap_rm(rbdclient):
    rbdclient.snap_rm('test/test04.root@backup')
    rbdclient._rbd.assert_has_calls([
        mock.call(['snap', 'rm', 'test/test04.root@backup'])])


@mock.patch('subprocess.Popen')
def test_rbd_export_diff(popen, rbdclient, tmpdir):
    stdout = open(str(tmpdir / 'foobar'), 'wb+')
    stdout.write(RBDDiffV1.header)
    stdout.seek(0)
    popen.return_value = mock.Mock(stdout=stdout)
    with rbdclient.export_diff('test/test04.root@new', 'old') as diff:
        assert isinstance(diff, RBDDiffV1)
    popen.assert_has_calls([
        mock.call([RBD,
                  'export-diff', 'test/test04.root@new',
                   '--from-snap', 'old', '-'],
                  stdin=subprocess.DEVNULL,
                  stdout=subprocess.PIPE,
                  bufsize=mock.ANY,
                  ),
       ])


def test_rbd_image_reader(rbdclient, tmpdir):
    device = str(tmpdir / 'device')
    open(device, 'wb').write(b'asdf')
    rbdclient._rbd.side_effect = [
        None,
        {'1': {'device': device, 'pool': 'test',
               'name': 'test04.root', 'snap': 'foo'}},
        None]
    with rbdclient.image_reader('test/test04.root@foo') as f:
        assert f.name == device
    rbdclient._rbd.assert_has_calls([
        mock.call(['--read-only', 'map', 'test/test04.root@foo']),
        mock.call(['showmapped'], format='json'),
        mock.call(['unmap', device])])


def test_rbd_image_reader_explicit_closed(rbdclient, tmpdir):
    device = str(tmpdir / 'device')
    open(device, 'wb').write(b'asdf')
    rbdclient._rbd.side_effect = [
        None,
        {'1': {'device': device, 'pool': 'test',
               'name': 'test04.root', 'snap': 'foo'}},
        None]
    with rbdclient.image_reader('test/test04.root@foo') as f:
        f.close()
    rbdclient._rbd.assert_has_calls([
        mock.call(['--read-only', 'map', 'test/test04.root@foo']),
        mock.call(['showmapped'], format='json'),
        mock.call(['unmap', device])])


@mock.patch('subprocess.Popen')
def test_rbd_export(popen, rbdclient, tmpdir):
    stdout = open(str(tmpdir / 'foobar'), 'wb+')
    popen.return_value = mock.Mock(stdout=stdout)
    with rbdclient.export(mock.sentinel.image) as f:
        assert f == stdout
    popen.assert_has_calls([
        mock.call([RBD,
                  'export', mock.sentinel.image, '-'],
                  stdin=subprocess.DEVNULL,
                  stdout=subprocess.PIPE,
                  bufsize=mock.ANY,
                  ),
        ])
