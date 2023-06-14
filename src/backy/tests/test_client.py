import datetime
from unittest import mock

import pytest
from aiohttp import ClientResponseError, hdrs
from aiohttp.web_exceptions import HTTPUnauthorized

from backy import utils
from backy.api import BackyAPI
from backy.client import APIClient, CLIClient
from backy.revision import Revision
from backy.tests import Ellipsis

from ..quarantine import QuarantineReport
from .test_daemon import daemon


@pytest.fixture
def log(log):
    return log.bind(job_name="-")


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
    api_client = APIClient(
        "<server>", "http://localhost:0", "token", "task", log
    )
    await api_client.session.close()
    api_client.session = client
    return api_client


@pytest.fixture
async def cli_client(api_client, log):
    return CLIClient(api_client, log)


async def test_cli_jobs(cli_client, capsys):
    await cli_client.jobs()
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

    await cli_client.jobs(filter_re="test01")
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

    await cli_client.jobs(filter_re="asdf")
    out, err = capsys.readouterr()
    assert (
        Ellipsis(
            """\
┏━━━━━┳━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
┃     ┃     ┃         ┃        ┃ Last    ┃         ┃        ┃ Next    ┃        ┃
┃     ┃     ┃ SLA     ┃        ┃ Backup  ┃ Last    ┃   Last ┃ Backup  ┃ Next   ┃
┃ Job ┃ SLA ┃ overdue ┃ Status ┃ ... ┃ Tags    ┃ Durat… ┃ ... ┃ Tags   ┃
┡━━━━━╇━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
└─────┴─────┴─────────┴────────┴─────────┴─────────┴────────┴─────────┴────────┘
0 jobs shown
"""
        )
        == out
    )


async def test_cli_status(cli_client, capsys):
    await cli_client.status()
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


async def test_cli_run(daemon, cli_client, monkeypatch):
    utils.log_data = ""
    run = mock.Mock()
    monkeypatch.setattr(daemon.jobs["test01"].run_immediately, "set", run)

    await cli_client.run("test01")

    run.assert_called_once()

    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/jobs/test01/run' query=''
... I ~cli[ABCD]           api/get-job                    name='test01'
... I ~cli[ABCD]           api/run-job                    name='test01'
... D ~cli[ABCD]           api/request-result             status_code=202
... I -                    CLIClient/triggered-run        job='test01'
"""
        )
        == utils.log_data
    )


async def test_cli_run_missing(daemon, cli_client):
    utils.log_data = ""

    try:
        await cli_client.run("aaaa")
    except SystemExit as e:
        assert e.code == 1

    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/jobs/aaaa/run' query=''
... I ~cli[ABCD]           api/get-job                    name='aaaa'
... I ~cli[ABCD]           api/get-job-not-found          name='aaaa'
... D ~cli[ABCD]           api/request-result             status_code=404
... E -                    CLIClient/unknown-job          job='aaaa'
"""
        )
        == utils.log_data
    )


async def test_cli_runall(daemon, cli_client, monkeypatch):
    utils.log_data = ""
    run1 = mock.Mock()
    run2 = mock.Mock()
    monkeypatch.setattr(daemon.jobs["test01"].run_immediately, "set", run1)
    monkeypatch.setattr(daemon.jobs["foo00"].run_immediately, "set", run2)

    await cli_client.runall()

    run1.assert_called_once()
    run2.assert_called_once()
    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/jobs' query=''
... I ~cli[ABCD]           api/get-jobs                   \n\
... D ~cli[ABCD]           api/request-result             response=... status_code=200
... D ~[ABCD]              api/new-conn                   path='/v1/jobs/test01/run' query=''
... I ~cli[ABCD]           api/get-job                    name='test01'
... I ~cli[ABCD]           api/run-job                    name='test01'
... D ~cli[ABCD]           api/request-result             status_code=202
... I -                    CLIClient/triggered-run        job='test01'
... D ~[ABCD]              api/new-conn                   path='/v1/jobs/foo00/run' query=''
... I ~cli[ABCD]           api/get-job                    name='foo00'
... I ~cli[ABCD]           api/run-job                    name='foo00'
... D ~cli[ABCD]           api/request-result             status_code=202
... I -                    CLIClient/triggered-run        job='foo00'
"""
        )
        == utils.log_data
    )


async def test_cli_reload(daemon, cli_client, monkeypatch):
    utils.log_data = ""
    reload = mock.Mock()
    monkeypatch.setattr(daemon, "reload", reload)

    await cli_client.reload()

    reload.assert_called_once()
    assert (
        Ellipsis(
            """\
... I -                    CLIClient/reloading-daemon     \n\
... D ~[ABCD]              api/new-conn                   path='/v1/reload' query=''
... I ~cli[ABCD]           api/reload-daemon              \n\
... D ~cli[ABCD]           api/request-result             status_code=204
... I -                    CLIClient/reloaded-daemon      \n\
"""
        )
        == utils.log_data
    )


async def test_cli_check_ok(daemon, cli_client):
    utils.log_data = ""
    try:
        await cli_client.check()
    except SystemExit as e:
        assert e.code == 0
    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/status' query='filter='
... I ~cli[ABCD]           api/get-status                 filter=''
... D ~cli[ABCD]           api/request-result             response=... status_code=200
... I -                    CLIClient/check-exit           exitcode=0 jobs=2
"""
        )
        == utils.log_data
    )


async def test_cli_check_too_old(daemon, clock, cli_client, log):
    job = daemon.jobs["test01"]
    revision = Revision.create(job.backup, set(), log)
    revision.timestamp = utils.now() - datetime.timedelta(hours=48)
    revision.stats["duration"] = 60.0
    revision.materialize()

    utils.log_data = ""
    try:
        await cli_client.check()
    except SystemExit as e:
        assert e.code == 2
    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/status' query='filter='
... I ~cli[ABCD]           api/get-status                 filter=''
... D ~cli[ABCD]           api/request-result             response=... status_code=200
... C test01               CLIClient/check-sla-violation  last_time='2015-08-30 07:06:47+00:00' sla_overdue=172800.0
... I -                    CLIClient/check-exit           exitcode=2 jobs=2
"""
        )
        == utils.log_data
    )


async def test_cli_check_manual_tags(daemon, cli_client, log):
    job = daemon.jobs["test01"]
    revision = Revision.create(job.backup, {"manual:test"}, log)
    revision.stats["duration"] = 60.0
    revision.materialize()

    utils.log_data = ""
    try:
        await cli_client.check()
    except SystemExit as e:
        assert e.code == 0
    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/status' query='filter='
... I ~cli[ABCD]           api/get-status                 filter=''
... D ~cli[ABCD]           api/request-result             response=... status_code=200
... I test01               CLIClient/check-manual-tags    manual_tags='manual:test'
... I -                    CLIClient/check-exit           exitcode=0 jobs=2
"""
        )
        == utils.log_data
    )


async def test_cli_check_quarantine(daemon, cli_client, log):
    job = daemon.jobs["test01"]
    job.backup.quarantine.add_report(QuarantineReport(b"a", b"b", 0))

    utils.log_data = ""
    try:
        await cli_client.check()
    except SystemExit as e:
        assert e.code == 1
    assert (
        Ellipsis(
            """\
... D ~[ABCD]              api/new-conn                   path='/v1/status' query='filter='
... I ~cli[ABCD]           api/get-status                 filter=''
... D ~cli[ABCD]           api/request-result             response=... status_code=200
... W test01               CLIClient/check-quarantined    reports=1
... I -                    CLIClient/check-exit           exitcode=1 jobs=2
"""
        )
        == utils.log_data
    )
