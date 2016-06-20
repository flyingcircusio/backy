from backy.ext_deps import RBD
from backy.revision import Revision
from backy.sources.ceph.source import CephRBD
from backy.sources import select_source
from unittest.mock import Mock, call
import backy.utils
import os.path as p
import pytest
import subprocess
import time

BLOCK = backy.utils.PUNCH_SIZE

with open(p.join(p.dirname(__file__), 'nodata.rbddiff'), 'rb') as f:
    SAMPLE_RBDDIFF = f.read()


@pytest.fixture
def check_output(monkeypatch):
    check_output = Mock()
    check_output.return_value = b'{}'
    monkeypatch.setattr(subprocess, 'check_output', check_output)
    return check_output


@pytest.fixture
def nosleep(monkeypatch):
    monkeypatch.setattr(time, 'sleep', lambda x: None)


def test_select_ceph_source():
    assert select_source('ceph-rbd') == CephRBD


def test_assign_revision():
    source = CephRBD(dict(pool='test', image='foo'))
    revision = Mock()
    context_manager = source(revision)
    assert context_manager.revision is revision


def test_context_manager(check_output, backup):
    source = CephRBD(dict(pool='test', image='foo'))

    revision = Revision(1, backup.archive)
    with source(revision):
        pass

    assert check_output.call_args_list == [
        call([RBD, '--no-progress', 'snap', 'create',
              'test/foo@backy-1']),
        call([RBD, '--no-progress', '--format=json', 'snap', 'ls',
              'test/foo'])]


def test_context_manager_cleans_out_snapshots(check_output, backup, nosleep):
    source = CephRBD(dict(pool='test', image='foo'))

    check_output.side_effect = [
        # snap create
        b'{}',
        # snap ls
        b'[{"name": "someother"}, {"name": "backy-1"}, {"name": "backy-2"}]',
        # snap rm backy-2
        b'{}']

    revision = Revision('1', backup.archive)
    with source(revision):
        revision.materialize()
        revision.timestamp = backy.utils.now()
        revision.write_info()
        backup.archive.scan()

    assert check_output.call_args_list == [
        call([RBD, '--no-progress', 'snap', 'create',
              'test/foo@backy-1']),
        call([RBD, '--no-progress', '--format=json', 'snap', 'ls',
              'test/foo']),
        call([RBD, '--no-progress', 'snap', 'rm', 'test/foo@backy-2'])]


def test_choose_full_without_parent(check_output, backup):
    source = CephRBD(dict(pool='test', image='foo'))

    source.diff = Mock()
    source.full = Mock()

    revision = Revision('1', backup.archive)

    with source(revision):
        source.backup()

    assert not source.diff.called
    assert source.full.called


def test_choose_full_without_snapshot(check_output, backup):
    source = CephRBD(dict(pool='test', image='foo'))

    source.diff = Mock()
    source.full = Mock()

    revision1 = Revision('a1', backup.archive)
    revision1.timestamp = backy.utils.now()
    revision1.materialize()

    backup.archive.scan()

    revision2 = Revision('a2', backup.archive)
    revision2.parent = 'a1'

    with source(revision2):
        source.backup()

    assert not source.diff.called
    assert source.full.called


def test_choose_diff_with_snapshot(check_output, backup, nosleep):
    source = CephRBD(dict(pool='test', image='foo'))

    source.diff = Mock()
    source.full = Mock()

    revision1 = Revision('a1', backup.archive)
    revision1.timestamp = backy.utils.now()
    revision1.materialize()

    backup.archive.scan()

    revision2 = Revision('a2', backup.archive)
    revision2.parent = 'a1'

    check_output.side_effect = [
        # snap create
        b'{}',
        # snap ls
        b'[{"name": "backy-a1"}]',
        # snap ls
        b'[{"name": "backy-a1"}, {"name": "backy-a2"}]',
        # snap rm backy-a1
        b'{}']

    with source(revision2):
        source.backup()

    assert source.diff.called
    assert not source.full.called


def test_diff_backup(check_output, backup, tmpdir, nosleep):
    source = CephRBD(dict(pool='test', image='foo'))

    parent = Revision('ed968696-5ab0-4fe0-af1c-14cadab44661', backup.archive)
    parent.timestamp = backy.utils.now()
    parent.materialize()

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision(
        'f0e7292e-4ad8-4f2e-86d6-f40dca2aa802', backup.archive)
    revision.timestamp = backy.utils.now()
    revision.parent = parent.uuid

    with open(str(tmpdir / revision.parent), 'w') as f:
        f.write('asdf')

    with open(str(tmpdir / revision.uuid) + '.rbddiff', 'wb') as f:
        f.write(SAMPLE_RBDDIFF)

    backup.archive.scan()
    revision.materialize()

    check_output.side_effect = [
        # snap create
        b'{}',
        # export-diff
        b'{}',
        # snap ls
        b'[{"name": "backy-ed968696-5ab0-4fe0-af1c-14cadab44661"}, \
           {"name": "backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802"}]',
        # snap rm backy-ed96...
        b'{}']

    with source(revision):
        source.diff()

    assert check_output.call_args_list == [
        call([RBD, '--no-progress', 'snap', 'create',
             'test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802']),
        call([RBD, '--no-progress', 'export-diff',
              'test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802',
              '{}/f0e7292e-4ad8-4f2e-86d6-f40dca2aa802.rbddiff'.format(tmpdir),
              '--from-snap', 'backy-ed968696-5ab0-4fe0-af1c-14cadab44661']),
        call([RBD, '--no-progress', '--format=json', 'snap', 'ls',
              'test/foo']),
        call([RBD, '--no-progress', 'snap', 'rm',
              'test/foo@backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802'])]


def test_full_backup(check_output, backup, tmpdir):
    source = CephRBD(dict(pool='test', image='foo'))

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision('a0', backup.archive)
    revision.timestamp = backy.utils.now()
    revision.materialize()
    backup.archive.scan()

    rbd_source = str(tmpdir / '-dev-rbd0')
    with open(rbd_source, 'w') as f:
        f.write('Han likes Leia.')

    check_output.side_effect = [
        # snap create
        b'{}',
        # map
        b'{}',
        # showmapped
        '{{"rbd0": {{"pool": "test", "name": "foo", "snap": "backy-a0", \
                     "device": "{}"}}}}'.format(rbd_source).encode('ascii'),
        # unmap
        b'{}',
        # snap ls
        b'[{"name": "backy-a0"}]']

    with source(revision):
        source.full()

    assert check_output.call_args_list == [
        call([RBD, '--no-progress', 'snap', 'create',
              'test/foo@backy-a0']),
        call([RBD, '--no-progress', '--read-only', 'map',
              'test/foo@backy-a0']),
        call([RBD, '--no-progress', '--format=json', 'showmapped']),
        call([RBD, '--no-progress', 'unmap', rbd_source]),
        call([RBD, '--no-progress', '--format=json', 'snap', 'ls',
              'test/foo'])]


def test_full_backup_integrates_changes(check_output, backup, tmpdir, nosleep):
    # The backup source changes between two consecutive full backups. Both
    # backup images should reflect the state of the source at the time the
    # backup was run. This test is here to detect regressions while optimizing
    # the full backup algorithms (coping and applying deltas).
    source = CephRBD(dict(pool='test', image='foo'))
    content0 = BLOCK * b'A' + BLOCK * b'B' + BLOCK * b'C' + BLOCK * b'D'
    content1 = BLOCK * b'A' + BLOCK * b'X' + BLOCK * b'\0' + BLOCK * b'D'

    rev0 = Revision('a0', backup.archive)
    rev0.timestamp = backy.utils.now()
    rev0.materialize()
    backup.archive.scan()

    rev1 = Revision('a1', backup.archive)
    rev1.parent = 'a0'
    rev1.materialize()

    rbd_source = str(tmpdir / '_dev_rbd0')

    def returnvals(name):
        return [
            # snap create
            b'{}',
            # map
            b'{}',
            # showmapped
            '{{"rbd0": {{"pool": "test", "name": "foo", "snap": "backy-{}", \
                         "device": "{}"}}}}'.format(
                            name, rbd_source).encode('ascii'),
            # unmap
            b'{}',
            # snap ls
            '[{{"name": "backy-{}"}}]'.format(name).encode('ascii'),
        ]

    check_output.side_effect = (
        returnvals('a0') +
        returnvals('a1') +
        # snap rm
        [b'{}']
    )

    # check fidelity
    for content, rev in [(content0, rev0), (content1, rev1)]:
        with open(rbd_source, 'wb') as f:
            f.write(content)

        with source(rev):
            source.full()

        with open(rev.filename, 'rb') as f:
            assert content == f.read()


def test_verify_fail(check_output, backup, tmpdir):
    source = CephRBD(dict(pool='test', image='foo'))

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision('a0', backup.archive)
    revision.timestamp = backy.utils.now()
    revision.materialize()

    backup.archive.scan()

    rbd_source = str(tmpdir / '-dev-rbd0')
    with open(rbd_source, 'w') as f:
        f.write('Han likes Leia.')

    check_output.side_effect = [
        # snap create
        b'{}',
        # map
        b'{}',
        # showmapped
        '{{"rbd0": {{"pool": "test", "name": "foo", "snap": "backy-a0", \
                    "device": "{}"}}}}'.format(rbd_source).encode('ascii'),
        # unmap
        b'{}',
        # snap ls
        b'[{"name": "backy-a0"}]']

    # We never copied the data, so this has to fail.
    with source(revision):
        assert not source.verify()

    assert check_output.call_args_list == [
        call([RBD, '--no-progress', 'snap', 'create', 'test/foo@backy-a0']),
        call([RBD, '--no-progress', '--read-only', 'map',
              'test/foo@backy-a0']),
        call([RBD, '--no-progress', '--format=json', 'showmapped']),
        call([RBD, '--no-progress', 'unmap', rbd_source]),
        call([RBD, '--no-progress', '--format=json', 'snap', 'ls',
              'test/foo'])]


def test_verify(check_output, backup, tmpdir):
    source = CephRBD(dict(pool='test', image='foo'))

    # Those revision numbers are taken from the sample snapshot and need
    # to match, otherwise our diff integration will (correctly) complain.
    revision = Revision('a0', backup.archive)
    revision.timestamp = backy.utils.now()
    revision.materialize()

    backup.archive.scan()

    rbd_source = str(tmpdir / '-dev-rbd0')
    with open(rbd_source, 'w') as f:
        f.write('Han likes Leia.')

    target = str(tmpdir / 'a0')
    with open(target, 'w') as f:
        f.write('Han likes Leia.')

    check_output.side_effect = [
        # snap create
        b'{}',
        # map
        b'{}',
        # showmapped
        '{{"rbd0": {{"pool": "test", "name": "foo", "snap": "backy-a0", \
                    "device": "{}"}}}}'.format(rbd_source).encode('ascii'),
        # unmap
        b'{}',
        # snap ls
        b'[{"name": "backy-a0"}]']

    with source(revision):
        assert source.verify()

    assert check_output.call_args_list == [
        call([RBD, '--no-progress', 'snap', 'create', 'test/foo@backy-a0']),
        call([RBD, '--no-progress', '--read-only', 'map',
              'test/foo@backy-a0']),
        call([RBD, '--no-progress', '--format=json', 'showmapped']),
        call([RBD, '--no-progress', 'unmap', rbd_source]),
        call([RBD, '--no-progress', '--format=json', 'snap', 'ls',
              'test/foo'])]


def test_ceph_config_from_cli():
    s = CephRBD.config_from_cli('rbd/vm.root')
    assert s == {
        'pool': 'rbd',
        'image': 'vm.root',
    }


def test_ceph_config_from_cli_invalid():
    with pytest.raises(RuntimeError) as exc:
        CephRBD.config_from_cli('foobar')
    assert str(exc.value) == ('ceph source must be initialized with '
                              'POOL/IMAGE')
