import datetime
import os
import pprint
import sys
from functools import partialmethod

import pytest

import backy.repository
from backy import utils
from backy.revision import Revision
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
        backy.main.main()
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
        backy.main.main()
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
        backy.main.main()
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
        backy.main.main()
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
        backy.main.main()
    assert exit.value.code == 0


def print_args(*args, return_value=None, **kw):
    print(args)
    pprint.pprint(kw)
    return return_value


async def async_print_args(*args, **kw):
    print_args(*args, **kw)


def test_call_status(capsys, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, "status", print_args)
    argv.extend(["-v", "-b", str(backup.path), "status"])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.main.Command object at 0x...>,)
{'revision': 'all', 'yaml_': False}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            """\
... D command/invoked                     args='... -v -b ... status'
... D command/parsed                      func='status' func_args={'yaml_': False, 'revision': 'all'}
... D command/successful                  \n\
"""
        )
        == utils.log_data
    )


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
        backy.repository.Repository,
        "backup",
        partialmethod(print_args, return_value=success),
    )
    argv.extend(["-v", "backup", "manual:test"])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.repository.Repository object at 0x...>, {'manual:test'}, False)
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


def test_call_find(capsys, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, "find", print_args)
    argv.extend(["-v", "-b", str(backup.path), "find", "-r", "1"])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.main.Command object at ...>,)
{'revision': '1', 'uuid': False}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            """\
... D command/invoked                     args='... -v -b ... find -r 1'
... D command/parsed                      func='find' func_args={'uuid': False, 'revision': '1'}
... D command/successful                  \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


@pytest.mark.parametrize(
    ["action", "args"],
    [
        ("jobs", {"filter_re": "test"}),
        ("status", dict()),
        ("run", {"job": "test"}),
        ("runall", dict()),
        ("reload", dict()),
        ("check", dict()),
    ],
)
def test_call_client(
    capsys, backup, argv, monkeypatch, log, tmp_path, action, args
):
    monkeypatch.setattr(backy.client.CLIClient, action, async_print_args)
    conf = str(tmp_path / "conf")
    with open(conf, "w") as c:
        c.write(
            f"""\
global:
    base-dir: {str(tmp_path)}
api:
    addrs: "127.0.0.1, ::1"
    port: 1234
    cli-default:
        token: "test"

schedules: {{}}
jobs: {{}}
"""
        )

    argv.extend(["-v", "client", "-c", conf, action, *args.values()])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            f"""\
(<backy.client.CLIClient object at ...>,)
{args}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D command/invoked                     args='... -v client -c ... {action}{" "*bool(args)}{", ".join(args.values())}'
... D command/parsed                      func='client' func_args={{'config': PosixPath('...'), 'peer': None, \
'url': None, 'token': None{", "*bool(args)}{str(args)[1:-1]}, 'apifunc': '{action}'}}
... D daemon/read-config                  ...
... D command/return-code                 code=0
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


def test_call_scheduler(capsys, backup, argv, monkeypatch, tmp_path):
    monkeypatch.setattr(backy.main.Command, "scheduler", print_args)
    argv.extend(
        [
            "-v",
            "-b",
            str(backup.path),
            "-l",
            str(tmp_path / "backy.log"),
            "scheduler",
        ]
    )
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.main.Command object at ...>,)
{'config': PosixPath('/etc/backy.conf')}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            """\
... D command/invoked                     args='... -v -b ... scheduler'
... D command/parsed                      func='scheduler' func_args={'config': PosixPath('/etc/backy.conf')}
... D command/successful                  \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


@pytest.mark.parametrize("action", ["set", "add", "remove"])
def test_call_tags(capsys, backup, argv, monkeypatch, action):
    monkeypatch.setattr(backy.main.Command, "tags", print_args)
    argv.extend(
        ["-v", "-b", str(backup.path), "tags", action, "-r", "last", "manual:a"]
    )
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            f"""\
(<backy.main.Command object at ...>,)
{{'action': '{action}',
 'autoremove': False,
 'expect': None,
 'force': False,
 'revision': 'last',
 'tags': 'manual:a'}}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D quarantine/scan                     entries=0
... D command/invoked                     args='... -v -b ... tags {action} -r last manual:a'
... D command/parsed                      func='tags' func_args={{'autoremove': False, 'force': False, 'expect': None, \
'action': '{action}', 'revision': 'last', 'tags': 'manual:a'}}
... D command/successful                  \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


def test_call_expire(capsys, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, "expire", print_args)
    argv.extend(["-v", "-b", str(backup.path), "expire"])
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
(<backy.main.Command object at ...>,)
{}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            """\
... D quarantine/scan                     entries=0
... D command/invoked                     args='... -v -b ... expire'
... D command/parsed                      func='expire' func_args={}
... D command/successful                  \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


@pytest.mark.parametrize("action", ["pull", "push"])
def test_call_pull_push(capsys, backup, argv, monkeypatch, tmp_path, action):
    monkeypatch.setattr(backy.main.Command, action, print_args)
    conf = tmp_path / "conf"
    with open(conf, "w") as c:
        c.write(
            f"""\
global:
    base-dir: {str(tmp_path)}
api:
    addrs: "127.0.0.1, ::1"
    port: 1234
    cli-default:
        token: "test"
peers : {{}}
schedules: {{}}
jobs: {{}}
"""
        )

    argv.extend(["-v", "-b", str(backup.path), action, "-c", str(conf)])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            f"""\
(<backy.main.Command object at ...>,)
{{'config': {repr(conf)}}}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            f"""\
... D command/invoked                     args='... -v -b {backup.path} {action} -c {conf}'
... D command/parsed                      func='{action}' func_args={{'config': {repr(conf)}}}
... D command/successful                  \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


def test_call_unexpected_exception(
    capsys, backup, argv, monkeypatch, log, tmp_path
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.main.Command, "status", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    argv.extend(
        ["-l", str(tmp_path / "backy.log"), "-b", str(backup.path), "status"]
    )
    utils.log_data = ""
    with pytest.raises(SystemExit):
        backy.main.main()
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


def test_commands_wrapper_status(
    backup, tmp_path, capsys, clock, tz_berlin, log
):
    commands = backy.main.Command(tmp_path, "AAAA", log)

    revision1 = Revision.create(backup, {"daily"}, log, uuid="1")
    revision1.materialize()

    revision2 = Revision.create(backup, {"daily"}, log, uuid="2")
    revision2.timestamp = backy.utils.now() + datetime.timedelta(hours=1)
    revision2.server = "remote"
    revision2.orig_tags = {"daily"}
    revision2.materialize()

    revision3 = Revision.create(backup, {"new", "same"}, log, uuid="3")
    revision3.timestamp = backy.utils.now() + datetime.timedelta(hours=2)
    revision3.server = "remote"
    revision3.orig_tags = {"old", "same"}
    revision3.materialize()

    commands.status(yaml_=False, revision="all")
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
1 pending change(s) (Push changes with `backy push`)
"""
    )


def test_commands_wrapper_status_yaml(
    backup, tmp_path, capsys, clock, tz_berlin, log
):
    commands = backy.main.Command(tmp_path, "AAAA", log)

    revision = Revision.create(backup, set(), log, uuid="1")
    revision.stats["duration"] = 3.5
    revision.stats["bytes_written"] = 42
    revision.materialize()

    commands.status(yaml_=True, revision="all")
    out, err = capsys.readouterr()

    assert err == ""
    assert (
        out
        == f"""\
- orig_tags: []
  parent: ''
  server: ''
  stats:
    bytes_written: 42
    duration: 3.5
  tags: []
  timestamp: 2015-09-01 07:06:47+00:00
  trust: trusted
  uuid: '1'

"""
    )
