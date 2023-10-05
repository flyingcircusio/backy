import asyncio
import datetime
import os
import os.path as p
import re
import signal
import time
from pathlib import Path
from unittest import mock

import pytest
import yaml

from backy import utils
from backy.backends.chunked import ChunkedFileBackend
from backy.daemon import BackyDaemon
from backy.quarantine import QuarantineReport
from backy.revision import Revision
from backy.scheduler import Job
from backy.tests import Ellipsis


@pytest.fixture
async def daemon(tmpdir, event_loop, log):
    daemon = BackyDaemon(str(tmpdir / "config"), log)
    base_dir = str(tmpdir)
    source = str(tmpdir / "test01.source")
    with open(str(tmpdir / "config"), "w") as f:
        f.write(
            f"""\
---
global:
    base-dir: {base_dir}
    status-interval: 1
    telnet_port: 1234
    backup-completed-callback: {Path(__file__).parent / "test_callback.sh"}
schedules:
    default:
        daily:
            interval: 24h
            keep: 9
jobs:
    test01:
        source:
            type: file
            filename: {source}
        schedule: default
    foo00:
        source:
            type: file
            filename: {source}
        schedule: default
"""
        )

    with open(source, "w") as f:
        f.write("I am your father, Luke!")

    daemon.start(event_loop)
    yield daemon
    daemon.terminate()


def test_fail_on_nonexistent_config(log):
    daemon = BackyDaemon("/no/such/config", log)
    with pytest.raises(RuntimeError):
        daemon._read_config()


def test_reload(daemon, tmpdir):
    new_base_dir = p.join(tmpdir, "newdir")
    os.mkdir(new_base_dir)
    with open(str(tmpdir / "config"), "w") as f:
        f.write(
            f"""\
---
global:
    base-dir: {new_base_dir}
schedules:
    default2:
        daily:
            interval: 24h
            keep: 13
jobs:
    test05:
        source:
            type: file
            filename: {daemon.jobs["test01"].source}
        schedule: default2
    foo05:
        source:
            type: file
            filename: {daemon.jobs["foo00"].source}
        schedule: default2
"""
        )
    daemon.reload()
    assert daemon.base_dir == new_base_dir
    assert set(daemon.jobs) == {"test05", "foo05"}
    assert set(daemon.schedules) == {"default2"}
    assert daemon.telnet_port == BackyDaemon.telnet_port


@pytest.mark.asyncio
async def test_sighup(daemon, log, monkeypatch):
    """test that a `SIGHUP` causes a reload without interrupting other tasks"""

    def reload():
        nonlocal all_tasks
        all_tasks = asyncio.all_tasks()
        reloaded.set()

    async def send_sighup():
        os.kill(os.getpid(), signal.SIGHUP)

    all_tasks = []
    reloaded = asyncio.Event()
    monkeypatch.setattr(daemon, "reload", reload)

    signal_task = daemon.loop.create_task(send_sighup())
    await reloaded.wait()
    assert signal_task not in all_tasks


@pytest.mark.asyncio
async def test_run_backup(daemon, log):
    job = daemon.jobs["test01"]

    await job.run_backup({"manual:asdf"})
    job.backup.scan()
    assert len(job.backup.history) == 1
    revision = job.backup.history[0]
    assert revision.tags == {"manual:asdf"}
    backend = ChunkedFileBackend(revision, log)
    with backend.open("r") as f:
        assert f.read() == b"I am your father, Luke!"

    # Run again. This also covers the code path that works if
    # the target backup directory exists already.
    await job.run_backup({"manual:asdf"})
    job.backup.scan()
    assert len(job.backup.history) == 2
    revision = job.backup.history[1]
    assert revision.tags == {"manual:asdf"}
    backend = ChunkedFileBackend(revision, log)
    with backend.open("r") as f:
        assert f.read() == b"I am your father, Luke!"


async def test_run_callback(daemon, log):
    job = daemon.jobs["test01"]

    await job.run_backup({"manual:asdf"})
    await job.run_callback()

    with open("test01.callback_stdin", "r") as f:
        r = yaml.safe_load(f)[0]
        # see directory api before changing this
        assert isinstance(r["uuid"], str)
        assert isinstance(r["trust"], str)
        assert isinstance(r["timestamp"], datetime.datetime)
        assert isinstance(r["tags"][0], str)
        assert isinstance(r["stats"]["bytes_written"], int)
        assert isinstance(r["stats"]["duration"], float)
        # assert isinstance(r["location"], str)


def test_spread(daemon):
    job = daemon.jobs["test01"]
    assert job.spread == 19971
    job.name = "test02"
    assert job.spread == 36217
    job.name = "asdf02"
    assert job.spread == 14532


def test_sla_before_first_backup(daemon):
    job = daemon.jobs["test01"]
    # No previous backups - we consider this to be OK initially.
    # I agree that this gives us a blind spot in the beginning. I'll
    # think of something when this happens. Maybe keeping a log of errors
    # or so to notice that we tried previously.
    assert len(job.backup.history) == 0
    assert job.sla is True


def test_sla_over_time(daemon, clock, tmpdir, log):
    job = daemon.jobs["test01"]
    # No previous backups - we consider this to be OK initially.
    # I agree that this gives us a blind spot in the beginning. I'll
    # think of something when this happens. Maybe keeping a log of errors
    # or so to notice that we tried previously.
    revision = Revision(job.backup, log, "1")
    # We're on a 24h cycle. 6 hours old backup is fine.
    revision.timestamp = utils.now() - datetime.timedelta(hours=6)
    revision.stats["duration"] = 60.0
    revision.materialize()
    job.backup.scan()
    assert len(job.backup.history) == 1
    assert job.sla is True

    # 24 hours is also fine.
    revision.timestamp = utils.now() - datetime.timedelta(hours=24)
    revision.write_info()
    job.backup.scan()
    assert job.sla is True

    # 32 hours is also fine.
    revision.timestamp = utils.now() - datetime.timedelta(hours=32)
    revision.write_info()
    job.backup.scan()
    assert job.sla is True

    # 24*1.5 hours is the last time that is OK.
    revision.timestamp = utils.now() - datetime.timedelta(hours=24 * 1.5)
    revision.write_info()
    job.backup.scan()
    assert job.sla is True

    # 1 second later we consider this not to be good any longer.
    revision.timestamp = (
        utils.now()
        - datetime.timedelta(hours=24 * 1.5)
        - datetime.timedelta(seconds=1)
    )
    revision.write_info()
    job.backup.scan()
    assert job.sla is False


def test_incomplete_revs_dont_count_for_sla(daemon, clock, tmpdir, log):
    job = daemon.jobs["test01"]
    r1 = Revision(job.backup, log, "1")
    r1.timestamp = utils.now() - datetime.timedelta(hours=48)
    r1.stats["duration"] = 60.0
    r1.materialize()
    r2 = Revision(job.backup, log, "2")
    r2.timestamp = utils.now() - datetime.timedelta(hours=1)
    r2.materialize()
    job.backup.scan()
    assert False is job.sla


def test_status_should_default_to_basedir(daemon, tmpdir):
    assert str(tmpdir / "status") == daemon.status_file


def test_update_status(log):
    job = Job(mock.Mock(), "asdf", log)
    assert job.status == ""
    job.update_status("asdf")
    assert job.status == "asdf"


async def cancel_and_wait(job):
    while job._task.cancel():
        await asyncio.sleep(0.1)
    job._task = None


@pytest.mark.asyncio
async def test_task_generator(daemon, clock, tmpdir, monkeypatch, tz_berlin):
    # This is really just a smoke tests, but it covers the task pool,
    # so hey, better than nothing.

    for j in daemon.jobs.values():
        await cancel_and_wait(j)
    job = daemon.jobs["test01"]

    async def null_coroutine():
        return

    monkeypatch.setattr(job, "_wait_for_deadline", null_coroutine)

    # This patch causes a single run through the generator loop.
    def update_status(status):
        if status == "finished":
            job.stop()

    monkeypatch.setattr(job, "update_status", update_status)

    async def wait_for_job_finished():
        while job._task is not None:
            await asyncio.sleep(0.1)

    job.start()

    await wait_for_job_finished()


@pytest.mark.asyncio
async def test_task_generator_backoff(
    daemon, clock, tmpdir, monkeypatch, tz_berlin
):
    for j in daemon.jobs.values():
        await cancel_and_wait(j)
    job = daemon.jobs["test01"]

    async def null_coroutine():
        await asyncio.sleep(0.1)

    failures = [1, 1, 1]

    async def failing_coroutine(*args, **kw):
        await asyncio.sleep(0.1)
        if not failures:
            return
        f = failures.pop(0)
        if f:
            raise Exception()
        else:
            return

    monkeypatch.setattr(job, "_wait_for_deadline", null_coroutine)
    monkeypatch.setattr(job, "run_expiry", null_coroutine)
    monkeypatch.setattr(job, "run_purge", null_coroutine)
    monkeypatch.setattr(job, "run_callback", null_coroutine)
    monkeypatch.setattr(job, "run_backup", failing_coroutine)

    # This patch causes a single run through the generator loop.
    def update_status(status):
        if status == "finished":
            job.stop()

    monkeypatch.setattr(job, "update_status", update_status)

    async def wait_for_job_finished():
        while job._task is not None:
            await asyncio.sleep(0.1)

    utils.log_data = ""

    job.start()

    await wait_for_job_finished()

    assert (
        Ellipsis(
            """\
... D test01               job/loop-started               \n\
... I test01               job/waiting                    next_tags='daily' next_time='2015-09-02 07:32:51'
... E test01               job/exception                  exception_class='builtins.Exception' exception_msg=''
exception>\tTraceback (most recent call last):
exception>\t  File "/.../src/backy/scheduler.py", line ..., in run_forever
exception>\t    await self.run_backup(next_tags)
exception>\t  File "/.../src/backy/tests/test_daemon.py", line ..., in failing_coroutine
exception>\t    raise Exception()
exception>\tException
... W test01               job/backoff                    backoff=120
... I test01               job/waiting                    next_tags='daily' next_time='2015-09-01 09:08:47'
... E test01               job/exception                  exception_class='builtins.Exception' exception_msg=''
exception>\tTraceback (most recent call last):
exception>\t  File "/.../src/backy/scheduler.py", line ..., in run_forever
exception>\t    await self.run_backup(next_tags)
exception>\t  File "/.../src/backy/tests/test_daemon.py", line ..., in failing_coroutine
exception>\t    raise Exception()
exception>\tException
... W test01               job/backoff                    backoff=240
... I test01               job/waiting                    next_tags='daily' next_time='2015-09-01 09:10:47'
... E test01               job/exception                  exception_class='builtins.Exception' exception_msg=''
exception>\tTraceback (most recent call last):
exception>\t  File "/.../src/backy/scheduler.py", line ..., in run_forever
exception>\t    await self.run_backup(next_tags)
exception>\t  File "/.../src/backy/tests/test_daemon.py", line ..., in failing_coroutine
exception>\t    raise Exception()
exception>\tException
... W test01               job/backoff                    backoff=480
... I test01               job/waiting                    next_tags='daily' next_time='2015-09-01 09:14:47'
... I test01               job/stop                       \n\
... I test01               job/waiting                    next_tags='daily' next_time='2015-09-02 07:32:51'
"""
        )
        == utils.log_data
    )

    assert job.errors == 0
    assert job.backoff == 0


@pytest.mark.asyncio
async def test_write_status_file(daemon, event_loop):
    assert p.exists(daemon.status_file)
    first = os.stat(daemon.status_file)
    await asyncio.sleep(1.5)
    second = os.stat(daemon.status_file)
    assert first.st_mtime < second.st_mtime


def test_daemon_status(daemon):
    assert {"test01", "foo00"} == set([s["job"] for s in daemon.status()])


def test_daemon_status_filter_re(daemon):
    r = re.compile(r"foo\d\d")
    assert {"foo00"} == set([s["job"] for s in daemon.status(r)])


def test_check_ok(daemon, setup_structlog):
    setup_structlog.default_job_name = "-"
    daemon._write_status_file()

    utils.log_data = ""
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 0
    assert (
        Ellipsis(
            """\
... I -                    daemon/read-config             ...
... I -                    daemon/check-exit              exitcode=0 jobs=2
"""
        )
        == utils.log_data
    )


def test_check_too_old(daemon, tmpdir, clock, log, setup_structlog):
    setup_structlog.default_job_name = "-"
    job = daemon.jobs["test01"]
    revision = Revision(job.backup, log, "1")
    revision.timestamp = utils.now() - datetime.timedelta(hours=48)
    revision.stats["duration"] = 60.0
    revision.materialize()
    daemon._write_status_file()

    utils.log_data = ""
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 2
    assert (
        Ellipsis(
            """\
... I -                    daemon/read-config             ...
... C test01               daemon/check-sla-violation     last_time='2015-08-30 07:06:47+00:00' sla_overdue=172800.0
... I -                    daemon/check-exit              exitcode=2 jobs=2
"""
        )
        == utils.log_data
    )


def test_check_manual_tags(daemon, setup_structlog, log):
    setup_structlog.default_job_name = "-"
    job = daemon.jobs["test01"]
    revision = Revision.create(job.backup, {"manual:test"}, log)
    revision.timestamp = utils.now()
    revision.stats["duration"] = 60.0
    revision.materialize()
    daemon._write_status_file()

    utils.log_data = ""
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 0
    assert (
        Ellipsis(
            """\
... I -                    daemon/read-config             ...
... I test01               daemon/check-manual-tags       manual_tags='manual:test'
... I -                    daemon/check-exit              exitcode=0 jobs=2
"""
        )
        == utils.log_data
    )


def test_check_quarantine(daemon, setup_structlog, log):
    setup_structlog.default_job_name = "-"
    job = daemon.jobs["test01"]
    job.backup.quarantine.add_report(QuarantineReport(b"a", b"b", 0))
    daemon._write_status_file()

    utils.log_data = ""
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 1
    assert (
        Ellipsis(
            """\
... I -                    daemon/read-config             ...
... W test01               daemon/check-quarantined       reports=1
... I -                    daemon/check-exit              exitcode=1 jobs=2
"""
        )
        == utils.log_data
    )


def test_check_no_status_file(daemon, setup_structlog):
    setup_structlog.default_job_name = "-"
    os.unlink(daemon.status_file)

    utils.log_data = ""
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 3
    assert (
        Ellipsis(
            """\
... I -                    daemon/read-config             ...
... E -                    daemon/check-no-status-file    status_file='...'
"""
        )
        == utils.log_data
    )


def test_check_stale_status_file(daemon, setup_structlog):
    setup_structlog.default_job_name = "-"
    open(daemon.status_file, "a").close()
    os.utime(daemon.status_file, (time.time() - 301, time.time() - 301))

    utils.log_data = ""
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 2
    assert (
        Ellipsis(
            """\
... I -                    daemon/read-config             ...
... C -                    daemon/check-old-status-file   age=...
"""
        )
        == utils.log_data
    )
