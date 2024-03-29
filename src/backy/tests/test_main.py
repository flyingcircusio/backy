import os
import pprint
import sys

import pytest

import backy.backup
import backy.client
import backy.main
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
usage: pytest [-h] [-v] [-l LOGFILE] [-b BACKUPDIR]
              {client,backup,restore,purge,find,status,\
upgrade,scheduler,distrust,verify,forget}
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
usage: pytest [-h] [-v] [-l LOGFILE] [-b BACKUPDIR]
              {client,backup,restore,purge,find,status,\
upgrade,scheduler,distrust,verify,forget}
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


def print_args(*args, **kw):
    print(args)
    pprint.pprint(kw)


async def async_print_args(*args, **kw):
    print_args(*args, **kw)


def test_call_status(capsys, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, "status", print_args)
    argv.extend(["-v", "-b", backup.path, "status"])
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
... D command/invoked                args='... -v -b ... status'
... D command/parsed                 func='status' func_args={'yaml_': False, 'revision': 'all'}
... D command/successful             \n\
"""
        )
        == utils.log_data
    )


def test_call_backup(tmpdir, capsys, argv, monkeypatch):
    os.makedirs(str(tmpdir / "backy"))
    os.chdir(str(tmpdir / "backy"))

    with open(str(tmpdir / "backy" / "config"), "wb") as f:
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

    monkeypatch.setattr(backy.backup.Backup, "backup", print_args)
    argv.extend(["-v", "backup", "manual:test"])
    utils.log_data = ""
    with pytest.raises(SystemExit) as exit:
        backy.main.main()
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
            """\
... D command/invoked                args='... -v backup manual:test'
... D command/parsed                 func='backup' func_args={'force': False, 'tags': 'manual:test'}
... D quarantine/scan                entries=0
... D command/successful             \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


def test_call_find(capsys, backup, argv, monkeypatch):
    monkeypatch.setattr(backy.main.Command, "find", print_args)
    argv.extend(["-v", "-b", backup.path, "find", "-r", "1"])
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
... D command/invoked                args='... -v -b ... find -r 1'
... D command/parsed                 func='find' func_args={'uuid': False, 'revision': '1'}
... D command/successful             \n\
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
    capsys, backup, argv, monkeypatch, log, tmpdir, action, args
):
    monkeypatch.setattr(backy.client.CLIClient, action, async_print_args)
    conf = str(tmpdir / "conf")
    with open(conf, "w") as c:
        c.write(
            f"""\
global:
    base-dir: {str(tmpdir)}
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
... D command/invoked                args='... -v client -c ... {action}{" "*bool(args)}{", ".join(args.values())}'
... D command/parsed                 func='client' func_args={{'config': '...', 'peer': None, \
'url': None, 'token': None{", "*bool(args)}{str(args)[1:-1]}, 'apifunc': '{action}'}}
... D daemon/read-config             ...
... D command/successful             \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


def test_call_scheduler(capsys, backup, argv, monkeypatch, tmpdir):
    monkeypatch.setattr(backy.main.Command, "scheduler", print_args)
    argv.extend(
        ["-v", "-b", backup.path, "-l", str(tmpdir / "backy.log"), "scheduler"]
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
{'config': '/etc/backy.conf'}
"""
        )
        == out
    )
    assert (
        Ellipsis(
            """\
... D command/invoked                args='... -v -b ... scheduler'
... D command/parsed                 func='scheduler' func_args={'config': '/etc/backy.conf'}
... D command/successful             \n\
"""
        )
        == utils.log_data
    )
    assert exit.value.code == 0


def test_call_unexpected_exception(
    capsys, backup, argv, monkeypatch, log, tmpdir
):
    def do_raise(*args, **kw):
        raise RuntimeError("test")

    monkeypatch.setattr(backy.main.Command, "status", do_raise)
    import os

    monkeypatch.setattr(os, "_exit", lambda x: None)

    argv.extend(["-l", str(tmpdir / "backy.log"), "-b", backup.path, "status"])
    utils.log_data = ""
    with pytest.raises(SystemExit):
        backy.main.main()
    out, err = capsys.readouterr()
    assert "" == out
    assert (
        Ellipsis(
            """\
... D command/invoked                args='... -l ... -b ... status'
... D command/parsed                 func='status' func_args={'yaml_': False, 'revision': 'all'}
... E command/failed                 exception_class='builtins.RuntimeError' exception_msg='test'
exception>\tTraceback (most recent call last):
exception>\t  File ".../src/backy/main.py", line ..., in main
exception>\t    func(**func_args)
exception>\t  File ".../src/backy/tests/test_main.py", line ..., in do_raise
exception>\t    raise RuntimeError("test")
exception>\tRuntimeError: test
"""
        )
        == utils.log_data
    )


def test_commands_wrapper_status(backup, tmpdir, capsys, clock, tz_berlin, log):
    commands = backy.main.Command(str(tmpdir), log)

    revision = Revision(backup, log, "1")
    revision.timestamp = backy.utils.now()
    revision.materialize()

    commands.status(yaml_=False, revision="all")
    out, err = capsys.readouterr()

    assert err == ""
    assert out == Ellipsis(
        """\
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━┳━━━━━━━━━┓
┃ Date (...) ┃ ID ┃    Size ┃ Duration ┃ Tags ┃ Trust   ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━╇━━━━━━━━━┩
│ ...  │ 1  │ 0 Bytes │        - │      │ trusted │
└──────────────────────┴────┴─────────┴──────────┴──────┴─────────┘
1 revisions containing 0 Bytes data (estimated)
"""
    )


def test_commands_wrapper_status_yaml(
    backup, tmpdir, capsys, clock, tz_berlin, log
):
    commands = backy.main.Command(str(tmpdir), log)

    revision = Revision(backup, log, "1")
    revision.timestamp = backy.utils.now()
    revision.stats["duration"] = 3.5
    revision.stats["bytes_written"] = 42
    revision.materialize()

    commands.status(yaml_=True, revision="all")
    out, err = capsys.readouterr()

    assert err == ""
    assert (
        out
        == """\
- backend_type: chunked
  parent: ''
  stats:
    bytes_written: 42
    duration: 3.5
  tags: []
  timestamp: 2015-09-01 07:06:47+00:00
  trust: trusted
  uuid: '1'

"""
    )
