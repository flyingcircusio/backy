from backy.revision import Revision
from backy.tests import Ellipsis
import backy.backup
import backy.main
import os
import pprint
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
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert """\
usage: py.test [-h] [-v] [-l LOGFILE] [-b BACKUPDIR]
               {init,backup,restore,purge,find,status,nbd-server,\
upgrade,scheduler,check,distrust,verify}
               ...
""" == out
    assert err == ""


def test_display_help(capsys, argv):
    argv.append('--help')
    with pytest.raises(SystemExit) as exit:
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
usage: py.test [-h] [-v] [-l LOGFILE] [-b BACKUPDIR]
               {init,backup,restore,purge,find,status,nbd-server,\
upgrade,scheduler,check,distrust,verify}
               ...

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
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0


def print_args(*args, **kw):
    print(args)
    pprint.pprint(kw)


def test_call_status(capsys, caplog, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, 'status', print_args)
    argv.extend(['-v', '-b', backup.path, 'status'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Command object at 0x...>,)
{}
""") == out
    assert err == ""
    assert Ellipsis("""\
main.py   ... DEBUG    backup.status(**{})
""") == caplog.text


def test_call_init(capsys, caplog, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, 'init', print_args)
    argv.extend(['-v', '-b', backup.path, 'init', 'ceph-rbd', 'test/test04'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Command object at ...>,)
{'source': 'test/test04', 'type': 'ceph-rbd'}
""") == out
    assert err == ""
    assert Ellipsis("""\
main.py   ... DEBUG    backup.init(**{...ceph-rbd...})
""") == caplog.text


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
        backy.main.main(enable_fault_handler=False)
    out, err = capsys.readouterr()
    assert "" == err
    assert Ellipsis("""\
(<backy.backup.Backup object at 0x...>, {'test'})
{}
""") == out
    assert Ellipsis("""\
main.py   ... DEBUG    backup.backup(**{'tags': 'test'})
backup.py ... DEBUG    Backup("...")
""") == caplog.text
    assert exit.value.code == 0


def test_call_find(capsys, caplog, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, 'find', print_args)
    argv.extend(['-v', '-b', backup.path, 'find', '-r', '1'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Command object at ...>,)
{'revision': '1'}
""") == out
    assert err == ""
    assert Ellipsis("""\
main.py   ... DEBUG    backup.find(...
""") == caplog.text
    assert exit.value.code == 0


def test_call_check(capsys, caplog, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, 'check', print_args)
    argv.extend(['-v', '-b', backup.path, 'check'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Command object at ...>,)
{'config': '/etc/backy.conf'}
""") == out
    assert err == ""
    assert Ellipsis("""\
main.py   ... DEBUG    backup.check(**{'config': \
'/etc/backy.conf'})
""") == caplog.text
    assert exit.value.code == 0


def test_call_scheduler(capsys, caplog, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, 'scheduler', print_args)
    argv.extend(['-b', backup.path, 'scheduler'])
    with pytest.raises(SystemExit) as exit:
        backy.main.main(enable_fault_handler=False)
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.main.Command object at ...>,)
{'config': '/etc/backy.conf'}
""") == out
    assert err == ""
    assert Ellipsis("""\
main.py   ... DEBUG    backup.scheduler(**{'config': \
'/etc/backy.conf'})
""") == caplog.text
    assert exit.value.code == 0


def test_call_unexpected_exception(capsys, backup, caplog, argv, monkeypatch):
    def do_raise(*args, **kw):
        raise RuntimeError("test")
    monkeypatch.setattr(backy.main.Command, 'status', do_raise)
    import os
    monkeypatch.setattr(os, '_exit', lambda x: None)
    import logging
    monkeypatch.setattr(logging, 'error', print_args)
    monkeypatch.setattr(logging, 'exception', print_args)
    argv.extend(['-l', 'logfile', '-b', backup.path, 'status'])
    with pytest.raises(SystemExit):
        backy.main.main(enable_fault_handler=False)
    out, err = capsys.readouterr()
    assert "" == out
    assert "Error: test\n" == err
    assert Ellipsis("""\
main.py   ... DEBUG    backup.status(**{})
main.py   ... ERROR    test
Traceback (most recent call last):
  File ".../main.py", line ..., in main
    func(**func_args)
  File ".../src/backy/tests/test_main.py", line ..., in do_raise
    raise RuntimeError("test")
RuntimeError: test
main.py   ... INFO     Backy operation failed.
""") == caplog.text


@pytest.fixture
def commands(tmpdir):
    commands = backy.main.Command(str(tmpdir))
    commands.init('file', str(tmpdir) + '/source')
    return commands


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
    b = backy.backup.Backup(str(tmpdir))
    revision = Revision(b, 1)
    revision.timestamp = backy.utils.now()
    revision.materialize()

    commands.status()
    out, err = capsys.readouterr()

    assert err == ""
    assert out == """\
+---------------------+----+---------+-------+------+---------+
| Date (UTC)          | ID |    Size | Durat | Tags | Trust   |
+---------------------+----+---------+-------+------+---------+
| 2015-09-01 07:06:47 | 1  | 0 Bytes |   0 s |      | trusted |
+---------------------+----+---------+-------+------+---------+
1 revisions containing 0 Bytes data (estimated)
"""
