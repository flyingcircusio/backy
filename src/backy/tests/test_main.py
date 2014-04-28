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
    print(args, kw)


def test_call_status(capfd, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'status', print_args)
    argv.append('-v')
    argv.append('status')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capfd.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at ...>,) {}
""") == out
    assert err == ""


def test_call_init(capfd, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'init', print_args)
    argv.append('-v')
    argv.append('init')
    argv.append('test/test04')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capfd.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at ...>,) {'source': 'test/test04'}
""") == out
    assert err == ""


def test_call_backup(capfd, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'backup', print_args)
    argv.append('-v')
    argv.append('backup')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capfd.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at ...>,) {}
""") == out
    assert err == ""


def test_call_maintenance(capfd, argv, monkeypatch):
    monkeypatch.setattr(backy.backup.Backup, 'maintenance', print_args)
    argv.append('-v')
    argv.append('maintenance')
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capfd.readouterr()
    assert Ellipsis("""\
(<backy.backup.Backup object at ...>,) {'keep': 1}
""") == out
    assert err == ""


def test_call_unexpected_exception(capsys, argv, monkeypatch):
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
    assert out == """\
('Unexpected exception',) {}
(RuntimeError('test',),) {}
"""
    assert err == ""
