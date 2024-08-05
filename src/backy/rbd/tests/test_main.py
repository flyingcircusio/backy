import os
import pprint
from functools import partialmethod

import pytest

import backy.rbd
from backy import utils
from backy.rbd import RBDSource
from backy.revision import Revision
from backy.source import CmdLineSource
from backy.tests import Ellipsis


@pytest.fixture
def source_on_disk(tmp_path, log):
    with open(tmp_path / "config", "w", encoding="utf-8") as f:
        f.write(
            f"""
---
path: "{tmp_path}"
schedule:
    daily:
        interval: 1d
        keep: 7
source:
    type: rbd
    pool: a
    image: b
"""
        )
    return CmdLineSource.load(tmp_path, log).create_source()


def test_display_usage(capsys):
    exit = RBDSource.main("backy-rbd")
    assert exit == 0
    out, err = capsys.readouterr()
    assert (
        """\
usage: pytest [-h] [-v] [-C WORKDIR] [-t TASKID]
              {backup,restore,gc,verify} ...
"""
        == out
    )
    assert err == ""


def test_display_help(capsys):
    with pytest.raises(SystemExit) as exit:
        RBDSource.main("backy-rbd", "--help")
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
usage: pytest [-h] [-v] [-C WORKDIR] [-t TASKID]
              {backup,restore,gc,verify} ...

The rbd plugin for backy. You should not call this directly. Use the backy
command instead.

positional arguments:
...
"""
        )
        == out
    )
    assert err == ""


def test_verbose_logging(capsys):
    # This is just a smoke test to ensure the appropriate code path
    # for -v is covered.
    exit = RBDSource.main("backy-rbd", "-v")
    assert exit == 0


def print_args(*args, return_value=None, **kw):
    print(", ".join(map(repr, args)))
    pprint.pprint(kw)
    return return_value


@pytest.mark.parametrize(
    ["args", "rv", "rc", "params"],
    [
        (
            ["backup", "asdf"],
            0,
            1,
            ["<backy.revision.Revision object at 0x...>"],
        ),
        (
            ["backup", "asdf"],
            1,
            0,
            ["<backy.revision.Revision object at 0x...>"],
        ),
        (
            ["restore", "asdf", "out.img"],
            None,
            0,
            [
                "<backy.revision.Revision object at 0x...>",
                "RBDRestoreArgs(target='out.img', backend=<RestoreBackend.AUTO: 'auto'>)",
            ],
        ),
        (
            ["restore", "asdf", "--backend", "python", "out.img"],
            None,
            0,
            [
                "<backy.revision.Revision object at 0x...>",
                "RBDRestoreArgs(target='out.img', backend=<RestoreBackend.PYTHON: 'python'>)",
            ],
        ),
        (["gc"], None, 0, []),
        (
            ["verify", "asdf"],
            None,
            0,
            ["<backy.revision.Revision object at 0x...>"],
        ),
    ],
)
def test_call_fun(
    args,
    rv,
    rc,
    params,
    source_on_disk,
    tmp_path,
    capsys,
    monkeypatch,
    log,
):
    os.chdir(tmp_path)

    Revision(source_on_disk.repository, log, uuid="asdf").materialize()

    monkeypatch.setattr(
        backy.rbd.RBDSource,
        args[0],
        partialmethod(print_args, return_value=rv),
    )
    utils.log_data = ""
    exit = RBDSource.main("backy-rbd", "-v", *args)
    assert exit == rc
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            f"""\
{", ".join(["<backy.rbd.RBDSource object at 0x...>", *params])}
{{}}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D -                    command/invoked                     args='backy-rbd -v {" ".join([ *args])}'
... D -                    repo/scan-reports                   entries=0
... D -                    command/return-code                 code={rc}
"""
        )
        == utils.log_data
    )


def test_call_unexpected_exception(
    capsys, source_on_disk, monkeypatch, log, tmp_path
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.rbd.RBDSource, "gc", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    utils.log_data = ""
    exit = RBDSource.main(
        "backy-rbd", "-C", str(source_on_disk.repository.path), "gc"
    )
    assert exit == 1
    out, err = capsys.readouterr()
    assert "" == out
    assert (
        Ellipsis(
            """\
... D -                    command/invoked                     args='backy-rbd -C ... gc'
... D -                    repo/scan-reports                   entries=0
... E -                    command/failed                      exception_class='builtins.RuntimeError' exception_msg='test'
exception>\tTraceback (most recent call last):
exception>\t  File ".../src/backy/source.py", line ..., in main
exception>\t    source.gc()
exception>\t  File ".../src/backy/rbd/tests/test_main.py", line ..., in do_raise
exception>\t    raise RuntimeError("test")
exception>\tRuntimeError: test
"""
        )
        == utils.log_data
    )
