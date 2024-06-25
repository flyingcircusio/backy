import datetime
import os
import pprint
import sys
from functools import partialmethod

import pytest

import backy.rbd
from backy import utils
from backy.rbd import main
from backy.tests import Ellipsis


@pytest.fixture
def argv():
    original = sys.argv
    new = original[:1]
    sys.argv = new
    yield new
    sys.argv = original


def test_display_usage(capsys, argv):
    with pytest.raises(SystemExit) as exit:
        main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        """\
usage: pytest [-h] [-v] [-l LOGFILE] [-b BACKUPDIR] [-t TASKID]
              {client,backup,restore,purge,find,status,\
upgrade,scheduler,distrust,verify,forget,tags,expire,push,pull}
              ...
"""
        == out
    )
    assert err == ""


def test_display_client_usage(capsys, argv):
    argv.append("client")
    with pytest.raises(SystemExit) as exit:
        main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        """\
usage: pytest client [-h] [-c CONFIG] [-p PEER] [--url URL] [--token TOKEN]
                     {jobs,status,run,runall,reload,check} ...
"""
        == out
    )
    assert err == ""


def test_display_help(capsys, argv):
    argv.append("--help")
    with pytest.raises(SystemExit) as exit:
        main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
usage: pytest [-h] [-v] [-l LOGFILE] [-b BACKUPDIR] [-t TASKID]
              {client,backup,restore,purge,find,status,\
upgrade,scheduler,distrust,verify,forget,tags,expire,push,pull}
              ...

Backup and restore for block devices.

positional arguments:
...
"""
        )
        == out
    )
    assert err == ""


def test_display_client_help(capsys, argv):
    argv.extend(["client", "--help"])
    with pytest.raises(SystemExit) as exit:
        main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
usage: pytest client [-h] [-c CONFIG] [-p PEER] [--url URL] [--token TOKEN]
                     {jobs,status,run,runall,reload,check} ...

positional arguments:
...
"""
        )
        == out
    )
    assert err == ""


def test_verbose_logging(capsys, argv):
    # This is just a smoke test to ensure the appropriate code path
    # for -v is covered.
    argv.extend(["-v"])
    with pytest.raises(SystemExit) as exit:
        main()
    assert exit.value.code == 0


def print_args(*args, return_value=None, **kw):
    print(args)
    pprint.pprint(kw)
    return return_value


async def async_print_args(*args, **kw):
    print_args(*args, **kw)


@pytest.mark.parametrize("success", [False, True])
def test_call_backup(success, tmp_path, capsys, argv, monkeypatch):
    os.makedirs(tmp_path / "backy")
    os.chdir(tmp_path / "backy")

    with open(tmp_path / "backy" / "config", "wb") as f:
        f.write(
            """
---
schedule:
    daily:
        interval: 1d
        keep: 7
source:
    type: file
    filename: {}
""".format(
                __file__
            ).encode(
                "utf-8"
            )
        )

    monkeypatch.setattr(
        backy.rbd.RbdBackup,
        "backup",
        partialmethod(print_args, return_value=success),
    )
    argv.extend(["-v", "backup", "manual:test"])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        main()
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.backup.Backup object at 0x...>, {'manual:test'}, False)
{}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D command/invoked                     args='... -v backup manual:test'
... D command/parsed                      func='backup' func_args={{'force': False, 'tags': 'manual:test'}}
... D quarantine/scan                     entries=0
... D command/return-code                 code={int(not success)}
"""
        )
        == utils.log_data
    )
    assert exit.value.code == int(not success)


# TODO: test call restore, verify, gc
def test_call_unexpected_exception(
    capsys, backup, argv, monkeypatch, log, tmp_path
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.rbd.RbdBackup, "gc", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    argv.extend(
        ["-l", str(tmp_path / "backy.log"), "-b", str(backup.path), "gc"]
    )
    utils.log_data = ""
    with pytest.raises(SystemExit):
        main()
    out, err = capsys.readouterr()
    assert "" == out
    assert (
        Ellipsis(
            """\
... D command/invoked                     args='... -l ... -b ... status'
... D command/parsed                      func='status' func_args={'yaml_': False, 'revision': 'all'}
... E command/failed                      exception_class='builtins.RuntimeError' exception_msg='test'
exception>\tTraceback (most recent call last):
exception>\t  File ".../src/backy/main.py", line ..., in main
exception>\t    ret = func(**func_args)
exception>\t  File ".../src/backy/tests/test_main.py", line ..., in do_raise
exception>\t    raise RuntimeError("test")
exception>\tRuntimeError: test
"""
        )
        == utils.log_data
    )
