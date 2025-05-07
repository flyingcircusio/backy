import datetime
import os
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import create_autospec

import pytest

import backy.cli
import backy.repository
import backy.source
from backy import utils
from backy.repository import Repository
from backy.revision import Revision
from backy.tests import Ellipsis


def test_display_usage(capsys, argv):
    with pytest.raises(SystemExit) as exit:
        backy.cli.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        """\
usage: pytest [-h] [-v] [-c CONFIG] [-C WORKDIR] [-n]
              [--jobs <job filter> | -a]
              {init,rev-parse,log,backup,restore,distrust,verify,rm,tags,gc,reports-list,reports-show,reports-delete,check,show-jobs,show-daemon,reload-daemon}
              ...
"""
        == out
    )
    assert err == ""


def test_display_help(capsys, argv):
    argv.append("--help")
    with pytest.raises(SystemExit) as exit:
        backy.cli.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
usage: pytest [-h] [-v] [-c CONFIG] [-C WORKDIR] [-n]
              [--jobs <job filter> | -a]
              {init,rev-parse,log,backup,restore,distrust,verify,rm,tags,gc,reports-list,reports-show,reports-delete,check,show-jobs,show-daemon,reload-daemon}
              ...

Backy command line client.

positional arguments:
...
"""
        )
        == out
    )
    assert err == ""


@dataclass
class Instance:
    cls: type

    def __eq__(self, other):
        return isinstance(other, self.cls)


@pytest.mark.parametrize(
    ["fun", "args", "rv", "rc", "params"],
    [
        (
            "rev_parse",
            ["rev-parse", "-r", "1"],
            None,
            0,
            {"repo": Instance(Repository), "revision": "1", "uuid": False},
        ),
        (
            "log_",
            ["log"],
            None,
            0,
            {"repo": Instance(Repository), "json_": False, "revision": "all"},
        ),
        (
            "backup",
            ["backup", "manual:test"],
            0,
            0,
            {
                "repos": [Instance(Repository)],
                "bg": False,
                "tags": "manual:test",
                "force": False,
            },
        ),
        (
            "backup",
            ["backup", "--force", "--bg", "manual:test"],
            1,
            1,
            {
                "repos": [Instance(Repository)],
                "bg": True,
                "tags": "manual:test",
                "force": True,
            },
        ),
        (
            "restore",
            ["restore", "-r", "1", "file", "out.bin"],
            None,
            0,
            {"revision": "1", "target": Path("out.bin")},
        ),
        (
            "distrust",
            ["distrust", "-r", "1"],
            None,
            0,
            {"repo": Instance(Repository), "revision": "1"},
        ),
        (
            "verify",
            ["verify", "-r", "1"],
            None,
            0,
            {"revision": "1"},
        ),
        (
            "rm",
            ["rm", "-r", "1"],
            None,
            0,
            {"repo": Instance(Repository), "revision": "1"},
        ),
        (
            "tags",
            ["tags", "set", "-r", "last", "manual:a"],
            None,
            0,
            {
                "repo": Instance(Repository),
                "tag_action": "set",
                "autoremove": False,
                "expect": None,
                "revision": "last",
                "tags": "manual:a",
                "force": False,
            },
        ),
        (
            "tags",
            [
                "tags",
                "remove",
                "-r",
                "last",
                "--autoremove",
                "--expect",
                "manual:b",
                "manual:a",
            ],
            None,
            0,
            {
                "repo": Instance(Repository),
                "tag_action": "remove",
                "autoremove": True,
                "expect": "manual:b",
                "revision": "last",
                "tags": "manual:a",
                "force": False,
            },
        ),
        (
            "tags",
            ["tags", "add", "-r", "last", "--force", "manual:a"],
            None,
            0,
            {
                "repo": Instance(Repository),
                "tag_action": "add",
                "autoremove": False,
                "expect": None,
                "revision": "last",
                "tags": "manual:a",
                "force": True,
            },
        ),
        (
            "gc",
            ["gc", "--expire"],
            None,
            0,
            {"repo": Instance(Repository), "expire": True, "local": False},
        ),
        (
            "reports_list",
            ["reports-list"],
            None,
            0,
            {"repo": Instance(Repository)},
        ),
        (
            "reports_show",
            ["reports-show"],
            None,
            0,
            {"repo": Instance(Repository), "reports": None},
        ),
        (
            "reports_delete",
            ["reports-delete", "--all-reports"],
            None,
            0,
            {"repo": Instance(Repository), "reports": None},
        ),
        (
            "check",
            ["check"],
            None,
            0,
            {"repo": Instance(Repository)},
        ),
        (
            "show_jobs",
            ["show-jobs"],
            None,
            0,
            {"repos": [Instance(Repository)]},
        ),
        (
            "show_daemon",
            ["show-daemon"],
            None,
            0,
            {},
        ),
        (
            "reload_daemon",
            ["reload-daemon"],
            None,
            0,
            {},
        ),
    ],
)
def test_call_fun(
    fun,
    args,
    rv,
    rc,
    params,
    argv,
    tmp_path,
    monkeypatch,
    log,
):
    path = tmp_path / "test00"
    path.mkdir()
    os.chdir(path)

    with open(path / "config", "w", encoding="utf-8") as f:
        f.write(
            f"""
---
path: "{path}"
schedule:
    daily:
        interval: 1d
        keep: 7
source:
    type: file
    filename: {__file__}
"""
        )

    mock = create_autospec(getattr(backy.cli.Command, fun), return_value=rv)
    monkeypatch.setattr(backy.cli.Command, fun, mock)
    argv.extend(["-v", "-C", str(path), *args])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.cli.main()
    assert exit.value.code == rc
    mock.assert_called_once()
    assert (Instance(backy.cli.Command),) == mock.call_args.args
    assert params == mock.call_args.kwargs

    expected = ""
    expected += f"... D -                    command/invoked                     args='... -v -C ... {' '.join(args)}'\n"
    expected += f"... D -                    command/call                        func='{fun}' func_args=...\n"
    if "repo" in params or "repos" in params:
        expected += "... D test00               repo/scan-reports                   entries=0\n"
    expected += f"... D -                    command/return-code                 code={rc}\n"

    assert Ellipsis(expected) == utils.log_data


def test_call_unexpected_exception(
    capsys, repository, argv, monkeypatch, log, tmp_path
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.cli.Command, "log_", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    argv.extend(
        [
            "-v",
            "-C",
            str(repository.path),
            "log",
        ]
    )
    utils.log_data = ""
    with pytest.raises(SystemExit):
        backy.cli.main()
    out, err = capsys.readouterr()
    assert "" == out
    assert (
        Ellipsis(
            """\
... D -                    command/invoked                     args='... -v -C ... log'
... D -                    command/call                        func='log_' func_args=...
... E -                    command/failed                      exception_class='builtins.RuntimeError' exception_msg='test'
exception>\tTraceback (most recent call last):
exception>\t  File ".../src/backy/cli/__init__.py", line ..., in __call__
exception>\t    ret = func(**kwargs)
exception>\t          ^^^^^^^^^^^^^^
exception>\t  File ".../src/backy/cli/tests/test_main.py", line ..., in do_raise
exception>\t    raise RuntimeError("test")
exception>\tRuntimeError: test
"""
        )
        == utils.log_data
    )


def test_commands_wrapper_status(
    repository, tmp_path, capsys, clock, tz_berlin, log
):
    commands = backy.cli.Command(
        tmp_path, tmp_path / "config", False, ".*", log
    )

    revision1 = Revision.create(repository, {"daily"}, log, uuid="1")
    revision1.materialize()

    revision2 = Revision.create(repository, {"daily"}, log, uuid="2")
    revision2.timestamp = backy.utils.now() + datetime.timedelta(hours=1)
    revision2.server = "remote"
    revision2.orig_tags = {"daily"}
    revision2.materialize()

    revision3 = Revision.create(repository, {"new", "same"}, log, uuid="3")
    revision3.timestamp = backy.utils.now() + datetime.timedelta(hours=2)
    revision3.server = "remote"
    revision3.orig_tags = {"old", "same"}
    revision3.materialize()

    repository.connect()
    commands.log_(repository, json_=False, revision="all")
    out, err = capsys.readouterr()

    assert err == ""
    assert out == Ellipsis(
        """\
┏━━━━━━━━━━━━━━━━┳━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
┃ Date           ┃    ┃         ┃          ┃                ┃         ┃        ┃
┃ (Europe/Berli… ┃ ID ┃    Size ┃ Duration ┃ Tags           ┃ Trust   ┃ Server ┃
┡━━━━━━━━━━━━━━━━╇━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
│ 2015-09-01     │ 1  │ 0 Bytes │        - │ daily          │ trusted │        │
│ 09:06:47       │    │         │          │                │         │        │
│ 2015-09-01     │ 2  │ 0 Bytes │        - │ daily          │ trusted │ remote │
│ 10:06:47       │    │         │          │                │         │        │
│ 2015-09-01     │ 3  │ 0 Bytes │        - │ +new,-old,same │ trusted │ remote │
│ 11:06:47       │    │         │          │                │         │        │
└────────────────┴────┴─────────┴──────────┴────────────────┴─────────┴────────┘
3 revisions containing 0 Bytes data (estimated)
"""
    )


def test_commands_wrapper_status_json(
    repository, tmp_path, capsys, clock, tz_berlin, log
):
    commands = backy.cli.Command(
        tmp_path, tmp_path / "config", False, ".*", log
    )

    revision = Revision.create(repository, set(), log, uuid="1")
    revision.stats["duration"] = 3.5
    revision.stats["bytes_written"] = 42
    revision.materialize()

    repository.connect()
    commands.log_(repository, json_=True, revision="all")
    out, err = capsys.readouterr()

    assert err == ""
    assert (
        out
        == """\
[{\
"uuid": "1", \
"timestamp": "2015-09-01T07:06:47+00:00", \
"parent": "", "stats": {"bytes_written": 42, "duration": 3.5}, \
"trust": "trusted", \
"tags": [], \
"orig_tags": [], \
"server": ""\
}]
"""
    )
