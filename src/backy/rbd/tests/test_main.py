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
usage: pytest [-h] [-v] [-b BACKUPDIR] [-t TASKID]
              {backup,restore,gc,verify} ...
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
usage: pytest [-h] [-v] [-b BACKUPDIR] [-t TASKID]
              {backup,restore,gc,verify} ...

Backup and restore for block devices.

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
        backy.rbd.RbdSource,
        "backup",
        partialmethod(print_args, return_value=success),
    )
    argv.extend(["-v", "backup", "asdf"])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        main()
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.rbd.backup.RbdRepository object at 0x...>, 'asdf')
{}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D -                    command/invoked                     args='... -v backup asdf'
... D -                    quarantine/scan                     entries=0
... D -                    command/return-code                 code={int(not success)}
"""
        )
        == utils.log_data
    )
    assert exit.value.code == int(not success)


# TODO: test call restore, verify, gc
def test_call_unexpected_exception(
    capsys, rbdrepository, argv, monkeypatch, log, tmp_path
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.rbd.RbdSource, "gc", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    argv.extend(["-b", str(rbdrepository.path), "gc"])
    utils.log_data = ""
    with pytest.raises(SystemExit):
        main()
    out, err = capsys.readouterr()
    print(utils.log_data)
    assert "" == out
    assert (
        Ellipsis(
            """\
... D -                    command/invoked                     args='... -b ... gc'
... D -                    quarantine/scan                     entries=0
... E -                    command/failed                      exception_class='builtins.RuntimeError' exception_msg='test'
exception>\tTraceback (most recent call last):
exception>\t  File ".../src/backy/rbd/__init__.py", line ..., in main
exception>\t    b.gc()
exception>\t  File ".../src/backy/rbd/tests/test_main.py", line ..., in do_raise
exception>\t    raise RuntimeError("test")
exception>\tRuntimeError: test
"""
        )
        == utils.log_data
    )
