from backy.revision import Revision
from backy.scheduler import Task, TaskPool
from backy.daemon import BackyDaemon
from unittest import mock
import asyncio
import backy.utils
import datetime
import os
import pytest


@pytest.fixture
def daemon(tmpdir):
    daemon = BackyDaemon(str(tmpdir / 'config'))
    base_dir = str(tmpdir)
    source = str(tmpdir / 'test01.source')
    with open(str(tmpdir / 'config'), 'w') as f:
        f.write("""\
---
global:
    base-dir: {base_dir}
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
""".format(**locals()))

    with open(source, 'w') as f:
        f.write('I am your father, Luke!')

    daemon._read_config()
    return daemon


@pytest.mark.asyncio
def test_run_backup(event_loop, daemon, forget_about_btrfs):
    job = daemon.jobs['test01']
    t = Task(job)
    t.tags = set(['asdf'])
    t.ideal_start = backy.utils.now()
    # Not having a a deadline set causes this to fail.
    task_future = asyncio.Future()
    yield from t.backup(task_future)
    assert len(job.archive.history) == 1
    assert job.archive.history[0].tags == set(['asdf'])
    with open(job.archive.history[0].filename, 'r') as f:
        assert f.read() == 'I am your father, Luke!'

    # Run again. This also covers the code path that works if
    # the target backup directory exists already.
    task_future = asyncio.Future()
    yield from t.backup(task_future)
    assert len(job.archive.history) == 2
    assert job.archive.history[1].tags == set(['asdf'])
    with open(job.archive.history[1].filename, 'r') as f:
        assert f.read() == 'I am your father, Luke!'


def test_spread(daemon):
    job = daemon.jobs['test01']
    assert job.spread == 19971
    job.name = 'test02'
    assert job.spread == 36217
    job.name = 'asdf02'
    assert job.spread == 14532


def test_sla_before_first_backup(daemon):
    job = daemon.jobs['test01']
    # No previous backups - we consider this to be OK initially.
    # I agree that this gives us a blind spot in the beginning. I'll
    # think of something when this happens. Maybe keeping a log of errors
    # or so to notice that we tried previously.
    assert len(job.archive.history) == 0
    assert job.sla is True


def test_sla_over_time(daemon, clock, tmpdir):
    job = daemon.jobs['test01']
    os.makedirs(str(tmpdir / 'test01'))
    # No previous backups - we consider this to be OK initially.
    # I agree that this gives us a blind spot in the beginning. I'll
    # think of something when this happens. Maybe keeping a log of errors
    # or so to notice that we tried previously.
    revision = Revision('1', job.archive)
    # We're on a 24h cycle. 6 hours old backup is fine.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=6)
    revision.stats['duration'] = 60.0
    revision.materialize()
    job.archive.scan()
    assert len(job.archive.history) == 1
    assert job.sla is True

    # 24 hours is also fine.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=24)
    revision.write_info()
    job.archive.scan()
    assert job.sla is True

    # 32 hours is also fine.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=32)
    revision.write_info()
    job.archive.scan()
    assert job.sla is True

    # 24*1.5 hours is the last time that is OK.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=24 * 1.5)
    revision.write_info()
    job.archive.scan()
    assert job.sla is True

    # 1 second later we consider this not to be good any longer.
    revision.timestamp = backy.utils.now() - datetime.timedelta(
        hours=24 * 1.5) - datetime.timedelta(seconds=1)
    revision.write_info()
    job.archive.scan()
    assert job.sla is False


def test_incomplete_revs_dont_count_for_sla(daemon, clock, tmpdir):
    job = daemon.jobs['test01']
    os.makedirs(str(tmpdir / 'test01'))
    r1 = Revision('1', job.archive)
    r1.timestamp = backy.utils.now() - datetime.timedelta(hours=48)
    r1.stats['duration'] = 60.0
    r1.materialize()
    r2 = Revision('2', job.archive)
    r2.timestamp = backy.utils.now() - datetime.timedelta(hours=1)
    r2.materialize()
    job.archive.scan()
    assert False is job.sla


def test_status_should_default_to_basedir(daemon, tmpdir):
    assert str(tmpdir / 'status') == daemon.status_file


def test_update_status(daemon, clock, tmpdir):
    job = daemon.jobs['test01']
    assert job.status == ''
    job.update_status('asdf')
    assert job.status == 'asdf'


@pytest.mark.asyncio
def test_task_generator(daemon, clock, tmpdir, event_loop, monkeypatch):
    # This is really just a smoke tests, but it covers the task pool,
    # so hey, better than nothing.

    task = mock.Mock()
    task.ideal_start = backy.utils.now()

    job = daemon.jobs['test01']
    job.start()

    daemon.taskpool = TaskPool(event_loop)

    @asyncio.coroutine
    def null_coroutine(self):
        return

    monkeypatch.setattr(Task, 'wait_for_deadline', null_coroutine)
    monkeypatch.setattr(Task, 'wait_for_finished', null_coroutine)

    # This patch causes a single run through the generator loop.
    def patched_update_status(status):
        if status == "finished":
            job.stop()
    job.update_status = patched_update_status

    @asyncio.coroutine
    def wait_for_job_finished():
        while job._generator_handle is not None:
            yield from asyncio.sleep(0.2)

    yield from wait_for_job_finished()
