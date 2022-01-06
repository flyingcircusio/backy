import asyncio
import datetime
import os
import os.path as p
import re
import time
from unittest import mock

import backy.utils
import pytest
from backy.backends.chunked import ChunkedFileBackend
from backy.daemon import BackyDaemon
from backy.revision import Revision
from backy.scheduler import Job
from backy.tests import Ellipsis


@pytest.yield_fixture
async def daemon(tmpdir, event_loop):
    daemon = BackyDaemon(str(tmpdir / 'config'))
    base_dir = str(tmpdir)
    source = str(tmpdir / 'test01.source')
    with open(str(tmpdir / 'config'), 'w') as f:
        f.write("""\
---
global:
    base-dir: {base_dir}
    status-interval: 1
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
""".format(**locals()))

    with open(source, 'w') as f:
        f.write('I am your father, Luke!')

    daemon.start(event_loop)
    yield daemon
    daemon.terminate()


def test_fail_on_nonexistent_config():
    daemon = BackyDaemon('/no/such/config')
    with pytest.raises(SystemExit):
        daemon._read_config()


@pytest.mark.asyncio
async def test_run_backup(daemon):
    job = daemon.jobs['test01']

    await job.run_backup(set(['asdf']))
    job.backup.scan()
    assert len(job.backup.history) == 1
    revision = job.backup.history[0]
    assert revision.tags == set(['asdf'])
    backend = ChunkedFileBackend(revision)
    with backend.open('r') as f:
        assert f.read() == b'I am your father, Luke!'

    # Run again. This also covers the code path that works if
    # the target backup directory exists already.
    await job.run_backup(set(['asdf']))
    job.backup.scan()
    assert len(job.backup.history) == 2
    revision = job.backup.history[1]
    assert revision.tags == set(['asdf'])
    backend = ChunkedFileBackend(revision)
    with backend.open('r') as f:
        assert f.read() == b'I am your father, Luke!'

    daemon.terminate()


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
    assert len(job.backup.history) == 0
    assert job.sla is True


def test_sla_over_time(daemon, clock, tmpdir):
    job = daemon.jobs['test01']
    # No previous backups - we consider this to be OK initially.
    # I agree that this gives us a blind spot in the beginning. I'll
    # think of something when this happens. Maybe keeping a log of errors
    # or so to notice that we tried previously.
    revision = Revision(job.backup, '1')
    # We're on a 24h cycle. 6 hours old backup is fine.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=6)
    revision.stats['duration'] = 60.0
    revision.materialize()
    job.backup.scan()
    assert len(job.backup.history) == 1
    assert job.sla is True

    # 24 hours is also fine.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=24)
    revision.write_info()
    job.backup.scan()
    assert job.sla is True

    # 32 hours is also fine.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=32)
    revision.write_info()
    job.backup.scan()
    assert job.sla is True

    # 24*1.5 hours is the last time that is OK.
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=24 * 1.5)
    revision.write_info()
    job.backup.scan()
    assert job.sla is True

    # 1 second later we consider this not to be good any longer.
    revision.timestamp = backy.utils.now() - datetime.timedelta(
        hours=24 * 1.5) - datetime.timedelta(seconds=1)
    revision.write_info()
    job.backup.scan()
    assert job.sla is False


def test_incomplete_revs_dont_count_for_sla(daemon, clock, tmpdir):
    job = daemon.jobs['test01']
    r1 = Revision(job.backup, '1')
    r1.timestamp = backy.utils.now() - datetime.timedelta(hours=48)
    r1.stats['duration'] = 60.0
    r1.materialize()
    r2 = Revision(job.backup, '2')
    r2.timestamp = backy.utils.now() - datetime.timedelta(hours=1)
    r2.materialize()
    job.backup.scan()
    assert False is job.sla


def test_status_should_default_to_basedir(daemon, tmpdir):
    assert str(tmpdir / 'status') == daemon.status_file


def test_update_status():
    job = Job(mock.Mock(), 'asdf')
    assert job.status == ''
    job.update_status('asdf')
    assert job.status == 'asdf'


async def cancel_and_wait(job):
    while job._task.cancel():
        await asyncio.sleep(0.1)
    job._task = None


@pytest.mark.asyncio
async def test_task_generator(daemon, clock, tmpdir, monkeypatch):
    # This is really just a smoke tests, but it covers the task pool,
    # so hey, better than nothing.

    for j in daemon.jobs.values():
        await cancel_and_wait(j)
    job = daemon.jobs['test01']

    async def null_coroutine():
        return

    monkeypatch.setattr(job, '_wait_for_deadline', null_coroutine)

    # This patch causes a single run through the generator loop.
    def update_status(status):
        if status == "finished":
            job.stop()

    monkeypatch.setattr(job, 'update_status', update_status)

    async def wait_for_job_finished():
        while job._task is not None:
            await asyncio.sleep(.1)

    job.start()

    await wait_for_job_finished()


@pytest.mark.asyncio
async def test_task_generator_backoff(caplog, daemon, clock, tmpdir,
                                      monkeypatch):

    for j in daemon.jobs.values():
        await cancel_and_wait(j)
    job = daemon.jobs['test01']

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

    monkeypatch.setattr(job, '_wait_for_deadline', null_coroutine)
    monkeypatch.setattr(job, 'run_expiry', null_coroutine)
    monkeypatch.setattr(job, 'run_purge', null_coroutine)
    monkeypatch.setattr(job, 'run_backup', failing_coroutine)

    # This patch causes a single run through the generator loop.
    def update_status(status):
        if status == "finished":
            job.stop()

    monkeypatch.setattr(job, 'update_status', update_status)

    async def wait_for_job_finished():
        while job._task is not None:
            await asyncio.sleep(.1)

    caplog.clear()

    job.start()

    await wait_for_job_finished()
    assert Ellipsis("""\
INFO     ... test01: started backup loop
INFO     ... test01: 2015-09-02 07:32:51, {'daily'}
ERROR    ...
Traceback (most recent call last):
  File "/.../src/backy/scheduler.py", line ..., in run_forever
    await self.run_backup(next_tags)
  File "/.../src/backy/tests/test_daemon.py", line ..., in failing_coroutine
    raise Exception()
Exception
WARNING  ... test01: retrying in 120 seconds
INFO     ... test01: 2015-09-01 09:08:47, {'daily'}
ERROR    ...
Traceback (most recent call last):
  File "/.../src/backy/scheduler.py", line ..., in run_forever
    await self.run_backup(next_tags)
  File "/.../src/backy/tests/test_daemon.py", line ..., in failing_coroutine
    raise Exception()
Exception
WARNING  ... test01: retrying in 240 seconds
INFO     ... test01: 2015-09-01 09:10:47, {'daily'}
ERROR    ...
Traceback (most recent call last):
  File "/.../src/backy/scheduler.py", line ..., in run_forever
    await self.run_backup(next_tags)
  File "/.../src/backy/tests/test_daemon.py", line ..., in failing_coroutine
    raise Exception()
Exception
WARNING  ... test01: retrying in 480 seconds
INFO     ... test01: 2015-09-01 09:14:47, {'daily'}
INFO     ... test01: finished
INFO     ... test01: shutting down
INFO     ... test01: 2015-09-02 07:32:51, {'daily'}
""") == caplog.text

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
    assert {'test01', 'foo00'} == set([s['job'] for s in daemon.status()])


def test_daemon_status_filter_re(daemon):
    r = re.compile(r'foo\d\d')
    assert {'foo00'} == set([s['job'] for s in daemon.status(r)])


def test_check_ok(daemon, capsys):
    daemon._write_status_file()
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 0
    out, err = capsys.readouterr()
    assert 'OK: 2 jobs within SLA\n' == out


def test_check_too_old(daemon, tmpdir, clock, capsys):
    job = daemon.jobs['test01']
    revision = Revision(job.backup, '1')
    revision.timestamp = backy.utils.now() - datetime.timedelta(hours=48)
    revision.stats['duration'] = 60.0
    revision.materialize()
    daemon._write_status_file()
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 2
    out, err = capsys.readouterr()
    assert out == """\
CRITICAL: 1 jobs not within SLA
test01 (last time: 2015-08-30 07:06:47+00:00, overdue: 172800.0)
"""


def test_check_no_status_file(daemon, capsys):
    os.unlink(daemon.status_file)
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 3
    out, err = capsys.readouterr()
    assert out.startswith('UNKNOWN: No status file found at')


def test_check_stale_status_file(daemon, capsys):
    open(daemon.status_file, 'a').close()
    os.utime(daemon.status_file, (time.time() - 301, time.time() - 301))
    try:
        daemon.check()
    except SystemExit as exit:
        assert exit.code == 2
    out, err = capsys.readouterr()
    assert out == 'CRITICAL: Status file is older than 5 minutes\n'
