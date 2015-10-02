from backy.revision import Revision
from backy.tests import Ellipsis
import backy.backup
import backy.main
import os
import pytest
import sys
import yaml


@pytest.yield_fixture
def argv():
    original = sys.argv
    new = original[:1]
    sys.argv = new
    yield new
    sys.argv = original


def test_display_usage(capsys, argv):
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert """\
usage: py.test [-h] [-v] [-b BACKUPDIR]
               {init,backup,restore,status,scheduler,check} ...
""" == out
    assert err == ""


def test_display_help(capsys, argv):
    argv.append('--help')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
usage: py.test [-h] [-v] [-b BACKUPDIR]
               {init,backup,restore,status,scheduler,check} ...

Backup and restore for block devices.

positional arguments:
...
""") == out
    assert err == ""


def test_verbose_logging(capsys, argv):
    # This is just a smoke test to ensure the appropriate code path
    # for -v is covered.
    argv.extend(['-v'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0


def print_args(*args, **kw):
    import pprint
    print(args)
    pprint.pprint(kw)


def test_call_status(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Commands, 'status', print_args)
    argv.append('-v')
    argv.append('status')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Commands object at 0x...>,)
{}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.status(**{})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()


def test_call_init(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Commands, 'init', print_args)
    argv.append('-v')
    argv.append('init')
    argv.append('ceph-rbd')
    argv.append('test/test04')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Commands object at ...>,)
{'source': 'test/test04', 'type': 'ceph-rbd'}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.init(**{...ceph-rbd...})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()


def test_call_backup(tmpdir, capsys, caplog, argv, monkeypatch):
    os.makedirs(str(tmpdir / 'backy'))
    os.chdir(str(tmpdir / 'backy'))

    with open(str(tmpdir / 'backy' / 'config'), 'wb') as f:
        f.write("""
---
type: file
filename: {}
""".format(__file__).encode("utf-8"))

    monkeypatch.setattr(backy.backup.Backup, 'backup', print_args)
    argv.extend(['-v', 'backup', 'test'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at 0x...>, 'test')
{}
""") == out
    assert "" == err
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.backup(**{'tags': 'test'})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()
    assert exit.value.code == 0


def test_call_check(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Commands, 'check', print_args)
    argv.append('-v')
    argv.append('check')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Commands object at ...>,)
{'config': '/etc/backy.conf'}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.check(**{'config': \
'/etc/backy.conf'})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()
    assert exit.value.code == 0


def test_call_scheduler(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Commands, 'scheduler', print_args)
    argv.append('scheduler')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Commands object at ...>,)
{'config': '/etc/backy.conf'}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.scheduler(**{'config': \
'/etc/backy.conf'})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()
    assert exit.value.code == 0


def test_call_unexpected_exception(capsys, caplog, argv, monkeypatch):
    def do_raise(*args, **kw):
        raise RuntimeError("test")
    monkeypatch.setattr(backy.main.Commands, 'status', do_raise)
    import os
    monkeypatch.setattr(os, '_exit', lambda x: None)
    import logging
    monkeypatch.setattr(logging, 'error', print_args)
    monkeypatch.setattr(logging, 'exception', print_args)
    argv.append('status')
    with pytest.raises(SystemExit):
        backy.main.main()
    out, err = capsys.readouterr()
    assert "" == out
    assert "" == err
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup("...")
main.py                    ... DEBUG    backup.status(**{})
main.py                    ... ERROR    Unexpected exception
main.py                    ... ERROR    test
Traceback (most recent call last):
  File ".../main.py", line ..., in main
    func(**func_args)
  File ".../src/backy/tests/test_main.py", line ..., in do_raise
    raise RuntimeError("test")
RuntimeError: test
main.py                    ... INFO     Backup failed.\


""") == caplog.text()


@pytest.fixture
def commands(tmpdir):
    commands = backy.main.Commands(str(tmpdir))
    commands.init('file', str(tmpdir) + '/source')
    return commands


def test_commands_wrapper(commands):
    assert isinstance(commands._backup, backy.backup.Backup)


def test_commands_wrapper_init(commands, tmpdir):
    with pytest.raises(RuntimeError):
        # Already initialized by fixture. Doing again causes exception.
        commands.init('file', str(tmpdir) + '/source')
    with open(str(tmpdir) + '/config') as f:
        config = yaml.load(f)
        assert config == {
            'filename': str(tmpdir) + '/source',
            'type': 'file'}


def test_commands_wrapper_status(commands, tmpdir, capsys, clock):
    revision = Revision(1, commands._backup.archive)
    revision.timestamp = backy.utils.now()
    revision.materialize()

    commands.status()
    out, err = capsys.readouterr()

    assert err == ""
    assert out == """\
+-------------------------+----+---------+----------+------+
| Date                    | ID |    Size | Duration | Tags |
+-------------------------+----+---------+----------+------+
| 2015-09-01 07:06:47 UTC | 1  | 0 Bytes |        0 |      |
+-------------------------+----+---------+----------+------+
== Summary
1 revisions
0 Bytes data (estimated)
"""
