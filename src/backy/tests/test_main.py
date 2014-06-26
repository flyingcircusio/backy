from backy.tests import Ellipsis
import backy.backup
import backy.main
import pytest
import sys


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
               {init,backup,restore,status,schedule} ...
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
               {init,backup,restore,status,schedule} ...

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
    monkeypatch.setattr(backy.backup.Commands, 'status', print_args)
    argv.append('-v')
    argv.append('status')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Commands object at 0x...>,)
{}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.status(**{})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()


def test_call_init(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Commands, 'init', print_args)
    argv.append('-v')
    argv.append('init')
    argv.append('ceph-rbd')
    argv.append('test/test04')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Commands object at ...>,)
{'source': 'test/test04', 'type': 'ceph-rbd'}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.init(**{...ceph-rbd...})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()


def test_call_backup(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'backup', print_args)
    argv.append('-v')
    argv.append('backup')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at 0x...>, '')
{}
""") == out
    assert "" == err
    assert Ellipsis("""\
backup.py                  ... DEBUG    Backup(".../backy")
main.py                    ... DEBUG    backup.backup(**{'force': ''})
main.py                    ... INFO     Backup complete.\


""") == caplog.text()


def test_call_unexpected_exception(capsys, caplog, argv, monkeypatch):
    def do_raise(*args, **kw):
        raise RuntimeError("test")
    monkeypatch.setattr(backy.backup.Commands, 'status', do_raise)
    import os
    monkeypatch.setattr(os, '_exit', lambda x: None)
    import logging
    monkeypatch.setattr(logging, 'error', print_args)
    monkeypatch.setattr(logging, 'exception', print_args)
    argv.append('status')
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
