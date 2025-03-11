import datetime
from unittest import mock

import pytest
from aiohttp import ClientResponseError, hdrs
from aiohttp.web_exceptions import HTTPUnauthorized

from backy import utils
from backy.cli import Command
from backy.daemon.api import BackyAPI, Client
from backy.report import ChunkMismatchReport
from backy.revision import Revision
from backy.tests import Ellipsis


@pytest.fixture
async def daemon(tmp_path, monkeypatch, log):
    # FIXME
    from backy.daemon.tests.test_daemon import daemon

    gen = daemon.__pytest_wrapped__.obj(tmp_path, monkeypatch, log)
    async for i in gen:
        yield i


@pytest.fixture
async def api(daemon, log):
    api = BackyAPI(daemon, log)
    api.tokens = daemon.api_tokens
    return api


@pytest.mark.parametrize(
    "token",
    [
        None,
        "",
        "asdf",
        "Bearer",
        "Bearer ",
        "Bearer a",
        "Bearer asdf",
        "Bearer cli",
    ],
)
@pytest.mark.parametrize("endpoint", ["/invalid", "/status"])
@pytest.mark.parametrize(
    "method", [hdrs.METH_GET, hdrs.METH_POST, hdrs.METH_TRACE]
)
async def test_api_wrong_token(api, token, method, endpoint, aiohttp_client):
    client = await aiohttp_client(
        api.app,
        headers={hdrs.AUTHORIZATION: token} if token is not None else {},
        raise_for_status=True,
    )
    with pytest.raises(ClientResponseError) as e:
        await client.request(method, endpoint)
    assert e.value.status == HTTPUnauthorized.status_code


@pytest.fixture
async def api_client(api, aiohttp_client, log):
    client = await aiohttp_client(
        api.app,
        headers={hdrs.AUTHORIZATION: "Bearer testtoken", "taskid": "ABCD"},
        raise_for_status=True,
    )
    api_client = Client("<server>", "http://localhost:0", "token", "task", log)
    await api_client.session.close()
    api_client.session = client
    return api_client


@pytest.fixture
async def command(tmp_path, api_client, log):
    cmd = Command(tmp_path, tmp_path / "config", False, ".*", log)
    cmd.api = api_client
    return cmd


async def test_show_jobs(command, capsys):
    exitcode = await command("show_jobs", {})
    assert exitcode == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
┏━━━━━━━━┳━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
┃        ┃     ┃        ┃        ┃ Last   ┃        ┃        ┃ Next    ┃        ┃
┃        ┃     ┃ SLA    ┃        ┃ Backup ┃ Last   ┃   Last ┃ Backup  ┃ Next   ┃
┃ Job    ┃ SLA ┃ overd… ┃ Status ┃ ... ┃ Tags   ┃ Durat… ┃ ... ┃ Tags   ┃
┡━━━━━━━━╇━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
│ foo00  │ OK  │ -      │ waiti… │ -      │        │      - │ ... │ daily  │
│        │     │        │ for    │        │        │        │ ... │        │
│        │     │        │ deadl… │        │        │        │         │        │
│ test01 │ OK  │ -      │ waiti… │ -      │        │      - │ ... │ daily  │
│        │     │        │ for    │        │        │        │ ... │        │
│        │     │        │ deadl… │        │        │        │         │        │
│ dead01 │ -   │ -      │ Dead   │ -      │        │      - │ -       │        │
└────────┴─────┴────────┴────────┴────────┴────────┴────────┴─────────┴────────┘
3 jobs shown
"""
        )
        == out
    )

    command.jobs = "test01"
    exitcode = await command("show_jobs", {})
    assert exitcode == 0
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
┏━━━━━━━━┳━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
┃        ┃     ┃        ┃        ┃ Last   ┃        ┃        ┃ Next    ┃        ┃
┃        ┃     ┃ SLA    ┃        ┃ Backup ┃ Last   ┃   Last ┃ Backup  ┃ Next   ┃
┃ Job    ┃ SLA ┃ overd… ┃ Status ┃ ... ┃ Tags   ┃ Durat… ┃ ... ┃ Tags   ┃
┡━━━━━━━━╇━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
│ test01 │ OK  │ -      │ waiti… │ -      │        │      - │ ... │ daily  │
│        │     │        │ for    │        │        │        │ ... │        │
│        │     │        │ deadl… │        │        │        │         │        │
└────────┴─────┴────────┴────────┴────────┴────────┴────────┴─────────┴────────┘
1 jobs shown
"""
        )
        == out
    )

    command.jobs = "asdf"
    exitcode = await command("show_jobs", {})
    assert exitcode == 1


async def test_show_daemon(command, capsys):
    command.jobs = None
    exitcode = await command("show_daemon", {})
    assert exitcode == 0
    out, err = capsys.readouterr()
    assert (
        """\
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━┓
┃ Status               ┃ # ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━┩
│ Dead                 │ 1 │
│ waiting for deadline │ 2 │
└──────────────────────┴───┘
"""
        == out
    )


async def test_backup_bg(daemon, command, monkeypatch):
    utils.log_data = ""
    run = mock.Mock()
    monkeypatch.setattr(daemon.jobs["test01"].run_immediately, "set", run)

    command.jobs = "test01"
    exitcode = await command(
        "backup", {"bg": True, "tags": "manual:a", "force": False}
    )
    assert exitcode == 0

    run.assert_called_once()

    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='backup' func_args={'bg': True, 'tags': 'manual:a', 'force': False}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=0
... D ~[ABCD]              api/new-conn                        path='/v1/jobs/test01/run' query=''
... I ~cli[ABCD]           api/get-job                         name='test01'
... I ~cli[ABCD]           api/run-job                         name='test01'
... D ~cli[ABCD]           api/request-result                  status_code=202
... I test01               command/triggered-run               \n\
... D -                    command/return-code                 code=0
"""
        )
        == utils.log_data
    )


async def test_backup_bg_missing(daemon, command):
    utils.log_data = ""

    command.jobs = "aaaa"
    exitcode = await command(
        "backup", {"bg": True, "tags": "manual:a", "force": False}
    )
    assert exitcode == 1


async def test_backup_bg_all(daemon, command, monkeypatch):
    utils.log_data = ""
    run1 = mock.Mock()
    run2 = mock.Mock()
    monkeypatch.setattr(daemon.jobs["test01"].run_immediately, "set", run1)
    monkeypatch.setattr(daemon.jobs["foo00"].run_immediately, "set", run2)

    exitcode = await command(
        "backup", {"bg": True, "tags": "manual:a", "force": False}
    )
    assert exitcode == 0

    run1.assert_called_once()
    run2.assert_called_once()
    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='backup' func_args={'bg': True, 'tags': 'manual:a', 'force': False}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=0
... D foo00                repo/scan-reports                   entries=0
... D ~[ABCD]              api/new-conn                        path='/v1/jobs/test01/run' query=''
... I ~cli[ABCD]           api/get-job                         name='test01'
... I ~cli[ABCD]           api/run-job                         name='test01'
... D ~cli[ABCD]           api/request-result                  status_code=202
... I test01               command/triggered-run               \n\
... D ~[ABCD]              api/new-conn                        path='/v1/jobs/foo00/run' query=''
... I ~cli[ABCD]           api/get-job                         name='foo00'
... I ~cli[ABCD]           api/run-job                         name='foo00'
... D ~cli[ABCD]           api/request-result                  status_code=202
... I foo00                command/triggered-run               \n\
... D -                    command/return-code                 code=0
"""
        )
        == utils.log_data
    )


async def test_reload(daemon, command, monkeypatch):
    utils.log_data = ""
    reload = mock.Mock()
    monkeypatch.setattr(daemon, "reload", reload)

    command.jobs = None
    exitcode = await command("reload_daemon", {})
    assert exitcode == 0

    reload.assert_called_once()
    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='reload_daemon' func_args={}
... D ~[ABCD]              api/new-conn                        path='/v1/reload' query=''
... I ~cli[ABCD]           api/reload-daemon                   \n\
... D ~cli[ABCD]           api/request-result                  status_code=204
... D -                    command/return-code                 code=0
"""
        )
        == utils.log_data
    )


async def test_check_ok(daemon, command):
    utils.log_data = ""
    exitcode = await command("check", {})
    assert exitcode == 0
    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='check' func_args={}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=0
... D foo00                repo/scan-reports                   entries=0
... D -                    command/return-code                 code=0
"""
        )
        == utils.log_data
    )


async def test_check_too_old(daemon, clock, command, log):
    job = daemon.jobs["test01"]
    revision = Revision.create(job.repository, set(), log)
    revision.timestamp = utils.now() - datetime.timedelta(hours=48)
    revision.stats["duration"] = 60.0
    revision.materialize()

    utils.log_data = ""
    exitcode = await command("check", {})
    assert exitcode == 2
    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='check' func_args={}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=0
... D foo00                repo/scan-reports                   entries=0
... C test01               command/check-sla-violation         last_time='2015-08-30 07:06:47+00:00' sla_overdue=172800.0
... D -                    command/return-code                 code=2
"""
        )
        == utils.log_data
    )


async def test_check_manual_tags(daemon, command, log):
    job = daemon.jobs["test01"]
    revision = Revision.create(job.repository, {"manual:test"}, log)
    revision.stats["duration"] = 60.0
    revision.materialize()

    utils.log_data = ""
    exitcode = await command("check", {})
    assert exitcode == 0
    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='check' func_args={}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=0
... D foo00                repo/scan-reports                   entries=0
... I test01               command/check-manual-tags           manual_tags='manual:test'
... D -                    command/return-code                 code=0
"""
        )
        == utils.log_data
    )


async def test_check_quarantine(daemon, command, log):
    job = daemon.jobs["test01"]
    report = ChunkMismatchReport(b"a", b"b", 0)
    job.repository.add_report(report)

    utils.log_data = ""
    exitcode = await command("check", {})
    assert exitcode == 1

    job.repository.report_path.joinpath(f"{report.uuid}.report").unlink()
    exitcode = await command("check", {})
    assert exitcode == 0
    assert (
        Ellipsis(
            """\
... D -                    command/call                        func='check' func_args={}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=1
... D foo00                repo/scan-reports                   entries=0
... W test01               command/check-reports               reports=1
... D -                    command/return-code                 code=1
... D -                    command/call                        func='check' func_args={}
... D ~[ABCD]              api/new-conn                        path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                        \n\
... D ~cli[ABCD]           api/request-result                  response=... status_code=200
... D test01               repo/scan-reports                   entries=0
... D foo00                repo/scan-reports                   entries=0
... D -                    command/return-code                 code=0
"""
        )
        == utils.log_data
    )
