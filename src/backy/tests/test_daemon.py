from backy.backends.chunked import ChunkedFileBackend
from backy.daemon import BackyDaemon
from backy.revision import Revision
from backy.scheduler import Task
from backy.tests import Ellipsis
from unittest import mock
import asyncio
import backy.utils
import datetime
import os
import os.path as p
import pytest
import re
import time


@pytest.yield_fixture
def daemon(tmpdir, event_loop):
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
    foo00:
        source:
            type: file
            filename: {source}
        schedule: default
""".format(**locals()))

    with open(source, 'w') as f:
        f.write('I am your father, Luke!')

    daemon._prepare(event_loop)
    yield daemon
    daemon.terminate()


def test_fail_on_nonexistent_config():
    daemon = BackyDaemon('/no/such/config')
    with pytest.raises(SystemExit):
        daemon._read_config()


def test_run_backup(daemon):
    job = daemon.jobs['test01']
    t = Task(job)
    t.tags = set(['asdf'])
    # Not having a a deadline set causes this to fail.
    t.ideal_start = backy.utils.now()
    t.backup()
    assert len(job.backup.history) == 1
    revision = job.backup.history[0]
    assert revision.tags == set(['asdf'])
    backend = ChunkedFileBackend(revision)
    with backend.open('r') as f:
        assert f.read() == b'I am your father, Luke!'

    # Run again. This also covers the code path that works if
    # the target backup directory exists already.
    t.backup()
    assert len(job.backup.history) == 2
    revision = job.backup.history[1]
    assert revision.tags == set(['asdf'])
    backend = ChunkedFileBackend(revision)
    with backend.open('r') as f:
        assert f.read() == b'I am your father, Luke!'


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


def test_update_status(daemon, clock, tmpdir):
    job = daemon.jobs['test01']
    assert job.status == ''
    job.update_status('asdf')
    assert job.status == 'asdf'


@pytest.mark.asyncio
def test_task_generator(daemon, clock, tmpdir, monkeypatch):
    # This is really just a smoke tests, but it covers the task pool,
    # so hey, better than nothing.

    task = mock.Mock()
    task.ideal_start = backy.utils.now()

    job = daemon.jobs['test01']
    job.start()

    @asyncio.coroutine
    def null_coroutine(self):
        return

    monkeypatch.setattr(Task, 'wait_for_deadline', null_coroutine)

    # This patch causes a single run through the generator loop.
    def patched_update_status(status):
        if status == "finished":
            job.stop()
    job.update_status = patched_update_status

    @asyncio.coroutine
    def wait_for_job_finished():
        while job._generator_handle is not None:
            yield from asyncio.sleep(.1)

    yield from wait_for_job_finished()


@pytest.mark.asyncio
def test_task_generator_backoff(caplog, daemon, clock, tmpdir, monkeypatch):
    # This is really just a smoke tests, but it covers the task pool,
    # so hey, better than nothing.

    task = mock.Mock()
    task.ideal_start = backy.utils.now()

    job = daemon.jobs['test01']
    job.start()

    @asyncio.coroutine
    def null_coroutine(self):
        return

    failures = [1, 2, 3]

    def failing_coroutine(self):
        if failures:
            failures.pop()
            return 1
        return 0

    monkeypatch.setattr(Task, 'wait_for_deadline', null_coroutine)
    monkeypatch.setattr(Task, 'backup', failing_coroutine)

    # This patch causes a single run through the generator loop.
    def patched_update_status(status):
        if status == "finished":
            job.stop()
    job.update_status = patched_update_status

    @asyncio.coroutine
    def wait_for_job_finished():
        while job._generator_handle is not None:
            yield from asyncio.sleep(.1)

    yield from wait_for_job_finished()
    assert Ellipsis("""\
... INFO     test01: started task generator loop
... INFO     test01: got deadline trigger
... WARNING  test01: retrying in 120 seconds
... INFO     test01: got deadline trigger
... WARNING  test01: retrying in 240 seconds
... INFO     test01: got deadline trigger
... WARNING  test01: retrying in 480 seconds
... INFO     test01: got deadline trigger
... INFO     test01: got deadline trigger
""") == caplog.text


@pytest.mark.asyncio
def test_write_status_file(daemon, event_loop):
    daemon.running = True
    daemon.status_interval = .01
    assert not p.exists(daemon.status_file)
    asyncio.async(daemon.save_status_file())
    yield from asyncio.sleep(.02)
    daemon.running = False
    yield from asyncio.sleep(.02)
    assert p.exists(daemon.status_file)


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


def test_check_tooold(daemon, clock, tmpdir, capsys):
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
test01 (last time: 2015-08-30 07:06:47 UTC)
"""


def test_check_no_status_file(daemon, capsys):
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
