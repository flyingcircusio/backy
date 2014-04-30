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
    assert out == """\
usage: py.test [-h] [-v] [-b BACKUPDIR] {init,backup,status,maintenance} ...
"""
    assert err == ""


def test_display_help(capsys, argv):
    argv.append('--help')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert out.startswith("""\
usage: py.test [-h] [-v] [-b BACKUPDIR] {init,backup,status,maintenance} ...

Backup and restore for block devices.

positional arguments:
""")
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
    monkeypatch.setattr(backy.backup.Backup, 'status', print_args)
    argv.append('-v')
    argv.append('status')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at 0x...>,)
{}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                   28 DEBUG    Backup(".../backy")
main.py                     95 DEBUG    backup.status(**{})
""") == caplog.text()


def test_call_init(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'init', print_args)
    argv.append('-v')
    argv.append('init')
    argv.append('ceph-rbd')
    argv.append('test/test04')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at ...>,)
{'source': 'test/test04', 'type': 'ceph-rbd'}
""") == out
    assert err == ""
    assert Ellipsis("""\
backup.py                   28 DEBUG    Backup(".../backy")
main.py                     95 DEBUG    backup.init(**{...ceph-rbd...})
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
(<backy.backup.Backup object at 0x...>,)
{}
""") == out
    assert "" == err
    assert Ellipsis("""\
backup.py                   28 DEBUG    Backup(".../backy")
main.py                     95 DEBUG    backup.backup(**{})
""") == caplog.text()


def test_call_maintenance(capsys, caplog, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'maintenance', print_args)
    argv.append('-v')
    argv.append('maintenance')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at 0x...>,)
{'keep': 1}
""") == out
    assert "" == err
    assert Ellipsis("""\
backup.py                   28 DEBUG    Backup(".../backy")
main.py                     95 DEBUG    backup.maintenance(**{'keep': 1})
""") == caplog.text()


def test_call_unexpected_exception(capsys, caplog, argv, monkeypatch):
    def do_raise(*args, **kw):
        raise RuntimeError("test")
    monkeypatch.setattr(backy.backup.Backup, 'status', do_raise)
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
backup.py                   28 DEBUG    Backup("...")
main.py                     95 DEBUG    backup.status(**{})
main.py                     99 ERROR    Unexpected exception
main.py                    100 ERROR    test
Traceback (most recent call last):
  File ".../main.py", line ..., in main
    func(**func_args)
  File ".../src/backy/tests/test_main.py", line ..., in do_raise
    raise RuntimeError("test")
RuntimeError: test
""") == caplog.text()
