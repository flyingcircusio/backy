import asyncio
import datetime
import json
import os
import re
import signal
from pathlib import Path
from unittest import mock

import pytest
import yaml

from backy import utils
from backy.daemon import BackyDaemon
from backy.daemon.scheduler import Job
from backy.file import FileSource
from backy.revision import Revision
from backy.tests import Ellipsis


@pytest.fixture
async def daemon(tmp_path, monkeypatch, log):
    daemon = BackyDaemon(tmp_path / "config", log)
    source = str(tmp_path / "test01.source")
    with open(str(tmp_path / "config"), "w") as f:
        f.write(
            f"""\
---
global:
    base-dir: {str(tmp_path)}
    backup-completed-callback: {Path(__file__).parent / "test_callback.sh"}
api:
    port: 1234
    tokens:
        "testtoken": "cli"
        "testtoken2": "cli2"
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

    tmp_path.joinpath("dead01").mkdir()
    with open(tmp_path / "dead01" / "config", "w") as f:
        json.dump(
            {
                "schedule": {},
                "source": {
                    "type": "file",
                    "filename": str(tmp_path / "config"),
                },
            },
            f,
        )

    async def null_coroutine():
        return

    with monkeypatch.context() as m:
        m.setattr(daemon, "purge_old_files", null_coroutine)
        m.setattr(daemon, "purge_pending_backups", null_coroutine)
        daemon.start(asyncio.get_running_loop())
    yield daemon
    daemon.terminate()


def test_fail_on_nonexistent_config(log):
    daemon = BackyDaemon(Path("/no/such/config"), log)
    with pytest.raises(RuntimeError):
        daemon._read_config()


def test_reload(daemon, tmp_path):
    new_base_dir = tmp_path / "newdir"
    new_base_dir.mkdir()
    with open(str(tmp_path / "config"), "w") as f:
        f.write(
            f"""\
---
global:
    base-dir: {new_base_dir}
api:
    tokens:
        "newtoken": "cli"
        "newtoken2": "cli2"
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
    assert set(daemon.api_tokens) == {"newtoken", "newtoken2"}
    assert set(daemon.jobs) == {"test05", "foo05"}
    assert set(daemon.schedules) == {"default2"}
    assert daemon.api_port == BackyDaemon.api_port


async def test_sighup(daemon, log, monkeypatch):
    """test that a `SIGHUP` causes a reload without interrupting other tasks"""

    all_tasks = set()

    def reload():
        nonlocal all_tasks
        all_tasks = asyncio.all_tasks()
        reloaded.set()

    async def send_sighup():
        os.kill(os.getpid(), signal.SIGHUP)

    reloaded = asyncio.Event()
    monkeypatch.setattr(daemon, "reload", reload)

    signal_task = daemon.loop.create_task(send_sighup())
    await reloaded.wait()
    assert signal_task not in all_tasks


async def test_run_backup(daemon, log):
    job = daemon.jobs["test01"]

    await job.run_backup({"manual:asdf"})
    job.repository.scan()
    assert len(job.repository.history) == 1
    revision = job.repository.history[0]
    assert revision.tags == {"manual:asdf"}
    source = job.source.create_source(FileSource)
    with source._path_for_revision(revision).open("rb") as f:
        assert f.read() == b"I am your father, Luke!"

    # Run again. This also covers the code path that works if
    # the target backup directory exists already.
    await job.run_backup({"manual:asdf"})
    job.repository.scan()
    assert len(job.repository.history) == 2
    revision = job.repository.history[1]
    assert revision.tags == {"manual:asdf"}
    with source._path_for_revision(revision).open("rb") as f:
        assert f.read() == b"I am your father, Luke!"


async def test_run_callback(daemon, log):
    job = daemon.jobs["test01"]

    await job.run_backup({"manual:asdf"})
    job.repository.scan()
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
        assert isinstance(r["server"], str)


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
    assert len(job.repository.history) == 0
    assert job.sla is True


def test_sla_over_time(daemon, clock, tmp_path, log):
    job = daemon.jobs["test01"]
    # No previous backups - we consider this to be OK initially.
    # I agree that this gives us a blind spot in the beginning. I'll
    # think of something when this happens. Maybe keeping a log of errors
    # or so to notice that we tried previously.
    revision = Revision.create(job.repository, set(), log)
    # We're on a 24h cycle. 6 hours old backup is fine.
    revision.timestamp = utils.now() - datetime.timedelta(hours=6)
    revision.stats["duration"] = 60.0
    revision.materialize()
    job.repository.scan()
    assert len(job.repository.history) == 1
    assert job.sla is True

    # 24 hours is also fine.
    revision.timestamp = utils.now() - datetime.timedelta(hours=24)
    revision.write_info()
    job.repository.scan()
    assert job.sla is True

    # 32 hours is also fine.
    revision.timestamp = utils.now() - datetime.timedelta(hours=32)
    revision.write_info()
    job.repository.scan()
    assert job.sla is True

    # 24*1.5 hours is the last time that is OK.
    revision.timestamp = utils.now() - datetime.timedelta(hours=24 * 1.5)
    revision.write_info()
    job.repository.scan()
    assert job.sla is True

    # 1 second later we consider this not to be good any longer.
    revision.timestamp = (
        utils.now()
        - datetime.timedelta(hours=24 * 1.5)
        - datetime.timedelta(seconds=1)
    )
    revision.write_info()
    job.repository.scan()
    assert job.sla is False

    # a running backup does not influence this.
    job.update_status("running (slow)")
    r = Revision.create(job.repository, {"daily"}, log)
    r.write_info()
    assert job.sla is False


def test_incomplete_revs_dont_count_for_sla(daemon, clock, tmp_path, log):
    job = daemon.jobs["test01"]
    r1 = Revision.create(job.repository, set(), log)
    r1.timestamp = utils.now() - datetime.timedelta(hours=48)
    r1.stats["duration"] = 60.0
    r1.materialize()
    r2 = Revision.create(job.repository, set(), log)
    r2.timestamp = utils.now() - datetime.timedelta(hours=1)
    r2.materialize()
    job.repository.scan()
    assert False is job.sla


def test_update_status(daemon, log):
    job = Job(daemon, "asdf", log)
    assert job.status == ""
    job.update_status("asdf")
    assert job.status == "asdf"


async def cancel_and_wait(job):
    while job._task.cancel():
        await asyncio.sleep(0.1)
    job._task = None


async def test_task_generator(daemon, clock, tmp_path, monkeypatch, tz_berlin):
    # This is really just a smoke tests, but it covers the task pool,
    # so hey, better than nothing.

    for j in daemon.jobs.values():
        await cancel_and_wait(j)
    job = daemon.jobs["test01"]

    async def null_coroutine(*args, **kw):
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


async def test_task_generator_backoff(
    daemon, clock, tmp_path, monkeypatch, tz_berlin
):
    for j in daemon.jobs.values():
        await cancel_and_wait(j)
    job = daemon.jobs["test01"]

    async def null_coroutine(*args, **kw):
        await asyncio.sleep(0.1)

    async def false_coroutine(*args, **kw):
        return False

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
    monkeypatch.setattr(job, "run_gc", null_coroutine)
    monkeypatch.setattr(job, "run_callback", null_coroutine)
    monkeypatch.setattr(job, "run_backup", failing_coroutine)
    monkeypatch.setattr(job, "pull_metadata", null_coroutine)
    monkeypatch.setattr(job, "push_metadata", null_coroutine)
    monkeypatch.setattr(job, "_wait_for_leader", false_coroutine)

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
... D test01[...]         job/loop-started                    \n\
... D test01[...]         repo/scan-reports                   entries=0
... I test01[...]         job/waiting                         next_tags='daily' next_time='2015-09-02 07:32:51'
... E test01[...]         job/exception                       exception_class='builtins.Exception' exception_msg=''
exception>\tTraceback (most recent call last):
exception>\t  File "/.../src/backy/daemon/scheduler.py", line ..., in run_forever
exception>\t    await self.run_backup(next_tags)
exception>\t  File "/.../src/backy/daemon/tests/test_daemon.py", line ..., in failing_coroutine
exception>\t    raise Exception()
exception>\tException
... W test01[...]         job/backoff                         backoff=120
... D test01[...]         repo/scan-reports                   entries=0
... I test01[...]         job/waiting                         next_tags='daily' next_time='2015-09-01 09:08:47'
... E test01[...]         job/exception                       exception_class='builtins.Exception' exception_msg=''
exception>\tTraceback (most recent call last):
exception>\t  File "/.../src/backy/daemon/scheduler.py", line ..., in run_forever
exception>\t    await self.run_backup(next_tags)
exception>\t  File "/.../src/backy/daemon/tests/test_daemon.py", line ..., in failing_coroutine
exception>\t    raise Exception()
exception>\tException
... W test01[...]         job/backoff                         backoff=240
... D test01[...]         repo/scan-reports                   entries=0
... I test01[...]         job/waiting                         next_tags='daily' next_time='2015-09-01 09:10:47'
... E test01[...]         job/exception                       exception_class='builtins.Exception' exception_msg=''
exception>\tTraceback (most recent call last):
exception>\t  File "/.../src/backy/daemon/scheduler.py", line ..., in run_forever
exception>\t    await self.run_backup(next_tags)
exception>\t  File "/.../src/backy/daemon/tests/test_daemon.py", line ..., in failing_coroutine
exception>\t    raise Exception()
exception>\tException
... W test01[...]         job/backoff                         backoff=480
... D test01[...]         repo/scan-reports                   entries=0
... I test01[...]         job/waiting                         next_tags='daily' next_time='2015-09-01 09:14:47'
... I test01[...]         job/stop                            \n\
... D test01[...]         repo/scan-reports                   entries=0
... I test01[...]         job/waiting                         next_tags='daily' next_time='2015-09-02 07:32:51'
"""
        )
        == utils.log_data
    )

    assert job.errors == 0
    assert job.backoff == 0


def test_daemon_status(daemon):
    assert {"test01", "foo00"} == set([s["job"] for s in daemon.status()])


def test_daemon_status_filter_re(daemon):
    r = re.compile(r"foo\d\d")
    assert {"foo00"} == set([s["job"] for s in daemon.status(r)])


async def test_purge_pending(daemon, monkeypatch):
    run_gc = mock.Mock()
    monkeypatch.setattr("backy.daemon.scheduler.Job.run_gc", run_gc)
    monkeypatch.setattr(
        "asyncio.sleep", mock.Mock(side_effect=asyncio.CancelledError())
    )

    daemon.jobs["test01"].repository.set_purge_pending()
    del daemon.jobs["test01"]

    with pytest.raises(asyncio.CancelledError):
        await daemon.purge_pending_backups()

    run_gc.assert_called_once()
