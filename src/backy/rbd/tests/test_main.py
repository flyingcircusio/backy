import datetime
import os
import pprint
import sys
from functools import partialmethod

import pytest

import backy.rbd
from backy import utils
from backy.rbd import main
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule
from backy.source import Source
from backy.tests import Ellipsis


@pytest.fixture
def argv():
    original = sys.argv
    new = original[:1]
    sys.argv = new
    yield new
    sys.argv = original


@pytest.fixture
def repository_on_disk(tmp_path, log):
    with open(tmp_path / "config", "w", encoding="utf-8") as f:
        f.write(
            f"""
---
path: "{tmp_path}"
schedule:
    daily:
        interval: 1d
        keep: 7
type: rbd
"""
        )
    with open(tmp_path / "source.config", "w", encoding="utf-8") as f:
        f.write(
            """
---
type: rbd
pool: a
image: b
"""
        )
    repo = Repository(tmp_path, Source, Schedule(), log)
    repo.connect()
    return repo


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
    print(", ".join(map(repr, args)))
    pprint.pprint(kw)
    return return_value


@pytest.mark.parametrize(
    ["fun", "args", "rv", "rc", "params"],
    [
        (
            "backup",
            ["asdf"],
            0,
            1,
            ["<backy.revision.Revision object at 0x...>"],
        ),
        (
            "backup",
            ["asdf"],
            1,
            0,
            ["<backy.revision.Revision object at 0x...>"],
        ),
        (
            "restore",
            ["asdf", "out.img"],
            None,
            0,
            [
                "<backy.revision.Revision object at 0x...>",
                "RestoreArgs(target='out.img', backend=<RestoreBackend.AUTO: 'auto'>)",
            ],
        ),
        (
            "restore",
            ["asdf", "--backend", "python", "out.img"],
            None,
            0,
            [
                "<backy.revision.Revision object at 0x...>",
                "RestoreArgs(target='out.img', backend=<RestoreBackend.PYTHON: 'python'>)",
            ],
        ),
        ("gc", [], None, 0, []),
        (
            "verify",
            ["asdf"],
            None,
            0,
            ["<backy.revision.Revision object at 0x...>"],
        ),
    ],
)
def test_call_fun(
    fun,
    args,
    rv,
    rc,
    params,
    repository_on_disk,
    tmp_path,
    capsys,
    argv,
    monkeypatch,
    log,
):
    os.chdir(tmp_path)

    Revision(repository_on_disk, log, uuid="asdf").materialize()

    monkeypatch.setattr(
        backy.rbd.source.RBDSource,
        fun,
        partialmethod(print_args, return_value=rv),
    )
    argv.extend(["-v", fun, *args])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        main()
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            f"""\
{", ".join(["<backy.rbd.source.RBDSource object at 0x...>", *params])}
{{}}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D -                    command/invoked                     args='... -v {" ".join([fun, *args])}'
... D -                    repo/scan-reports                   entries=0
... I -                    chunked-store/to-v2                 \n\
... I -                    chunked-store/to-v2-finished        \n\
... D -                    command/return-code                 code={rc}
"""
        )
        == utils.log_data
    )
    assert exit.value.code == rc


def test_call_unexpected_exception(
    capsys, repository_on_disk, argv, monkeypatch, log, tmp_path
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.rbd.RBDSource, "gc", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    argv.extend(["-b", str(repository_on_disk.path), "gc"])
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
... D -                    repo/scan-reports                   entries=0
... I -                    chunked-store/to-v2                 \n\
... I -                    chunked-store/to-v2-finished        \n\
... E -                    command/failed                      exception_class='builtins.RuntimeError' exception_msg='test'
exception>\tTraceback (most recent call last):
exception>\t  File ".../src/backy/rbd/__init__.py", line ..., in main
exception>\t    source.gc()
exception>\t  File ".../src/backy/rbd/tests/test_main.py", line ..., in do_raise
exception>\t    raise RuntimeError("test")
exception>\tRuntimeError: test
"""
        )
        == utils.log_data
    )
