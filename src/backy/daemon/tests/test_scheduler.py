import datetime
from unittest import mock

import pytest

import backy.utils
from backy.daemon.scheduler import Job


@pytest.fixture
def daemon(tmp_path):
    daemon = mock.Mock()
    daemon.base_dir = tmp_path
    return daemon


async def test_wait_for_deadline_no_deadline_fails(daemon, log):
    job = Job(daemon, "dummy", log)
    # Not having a deadline set causes this to fail (immediately)
    with pytest.raises(AssertionError):
        await job._wait_for_deadline()


async def test_wait_for_deadline(daemon, log):
    job = Job(daemon, "dummy", log)
    # Not having a deadline set causes this to fail.
    now = backy.utils.now()
    job.next_time = now + datetime.timedelta(seconds=0.3)
    result = await job._wait_for_deadline()
    assert result is None  # The deadline sleep was triggered.
    assert backy.utils.now() - now >= datetime.timedelta(seconds=0.3)


async def test_wait_for_deadline_1000(daemon, log):
    job = Job(daemon, "dummy", log)
    # Large deadline
    now = backy.utils.now()
    job.next_time = now + datetime.timedelta(seconds=1000)
    job.run_immediately.set()
    result = await job._wait_for_deadline()
    assert result is True  # The event was triggered
    assert not job.run_immediately.is_set()  # The event was reset.
    assert backy.utils.now() - now < datetime.timedelta(seconds=0.5)
