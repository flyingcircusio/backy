import datetime
from unittest import mock

import backy.utils
import pytest
from backy.scheduler import Job


@pytest.mark.asyncio
async def test_wait_for_deadline_no_deadline_fails():
    daemon = mock.Mock()
    job = Job(daemon, 'dummy')
    # Not having a a deadline set causes this to fail (immediately)
    with pytest.raises(TypeError):
        await job._wait_for_deadline()


@pytest.mark.asyncio
async def test_wait_for_deadline():
    daemon = mock.Mock()
    job = Job(daemon, 'dummy')
    # Not having a a deadline set causes this to fail.
    now = backy.utils.now()
    job.next_time = now + datetime.timedelta(seconds=0.3)
    result = await job._wait_for_deadline()
    assert result is None  # The deadline sleep was triggered.
    assert backy.utils.now() - now >= datetime.timedelta(seconds=0.3)


@pytest.mark.asyncio
async def test_wait_for_deadline_1000():
    daemon = mock.Mock()
    job = Job(daemon, 'dummy')
    # Large deadline
    now = backy.utils.now()
    job.next_time = now + datetime.timedelta(seconds=1000)
    job.run_immediately.set()
    result = await job._wait_for_deadline()
    assert result is True  # The event was triggered
    assert not job.run_immediately.is_set()  # The event was reset.
    assert backy.utils.now() - now < datetime.timedelta(seconds=0.5)
