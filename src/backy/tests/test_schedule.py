from backy.backup import Archive
from backy.schedule import parse_duration, Schedule
import backy.utils
from datetime import datetime
import mock
import pytest
import pytz
import time


def time_(*args):
    dt = datetime(*args)
    return time.mktime(dt.timetuple())


def test_parse_duration():
    assert parse_duration('1d') == 24 * 60 * 60
    assert parse_duration('2d') == 2 * 24 * 60 * 60
    assert parse_duration('1h') == 60 * 60
    assert parse_duration('3h') == 3 * 60 * 60
    assert parse_duration('10m') == 10 * 60
    assert parse_duration('25m') == 25 * 60
    assert parse_duration('10s') == 10
    assert parse_duration('5') == 5


@pytest.fixture
def schedule():
    schedule = Schedule()
    schedule.configure({'daily': {'interval': '1d', 'keep': 5}})
    return schedule


@pytest.fixture
def archive(tmpdir):
    a = Archive(str(tmpdir))
    a.scan = lambda: None
    return a


def test_tag_first_interval_after_now(schedule, archive, clock):
    assert ((datetime(2015, 9, 1, 23, 59, 59, tzinfo=pytz.UTC), ['daily']) ==
            schedule.next(backy.utils.now() + 60, 1, archive))


def test_tag_second_interval_after_now(schedule, archive, clock):
    assert ((datetime(2015, 9, 2, 23, 59, 59, tzinfo=pytz.UTC), ['daily']) ==
            schedule.next(backy.utils.now() + 25 * 60 * 60, 1, archive))


def test_tag_second_interval_with_different_spread(schedule, archive, clock):
    assert ((datetime(2015, 9, 2, 23, 59, 55, tzinfo=pytz.UTC), ['daily']) ==
            schedule.next(backy.utils.now() + 25 * 60 * 60, 5, archive))


def test_tag_catchup_not_needed_for_recent(schedule, archive, clock):
    # A recent backup does not cause catchup to be triggered.
    revision = mock.Mock()
    revision.timestamp = clock.now() - 15
    revision.tags = ['daily']
    archive.history.append(revision)
    assert ((datetime(2015, 9, 1, 7, 14, 59, tzinfo=pytz.UTC), []) ==
            schedule._next_catchup(clock.now(), 1, archive))
    # This in turn causes the main next() function to return the regular next
    # interval.

    assert (
        (datetime(2015, 9, 1, 23, 59, 59, tzinfo=pytz.UTC), ['daily']) ==
        schedule.next(clock.now(), 1, archive))


def test_tag_catchup_not_needed_for_very_old(schedule, archive, clock):
    # If a backup has been overdue for too long, we expect the
    # tag to be scheduled soon anyway and we do not catch up to avoid
    # overload issues.
    revision = mock.Mock()
    revision.timestamp = clock.now() - (24 * 60 * 60) * 1.6
    revision.tags = ['daily']
    archive.history.append(revision)
    assert ((datetime(2015, 9, 1, 7, 14, 59, tzinfo=pytz.UTC), []) ==
            schedule._next_catchup(clock.now(), 1, archive))
    # This in turn causes the main next() function to return the regular next
    # interval.
    assert (
        (datetime(2015, 9, 1, 23, 59, 59, tzinfo=pytz.UTC), ['daily']) ==
        schedule.next(clock.now(), 1, archive))


def test_tag_catchup_needed_for_recently_missed(schedule, archive, clock):
    revision = mock.Mock()
    # A catchup is needed because the daily backup has been longer than 1 day
    # but less than 1.5 days.
    revision.timestamp = clock.now() - (24 * 60 * 60) * 1.2
    revision.tags = ['daily']
    archive.history.append(revision)
    assert ((datetime(2015, 9, 1, 7, 14, 59, tzinfo=pytz.UTC), ['daily']) ==
            schedule._next_catchup(clock.now(), 1, archive))
    # This in turn causes the main next() function to also
    # return this date.
    assert (
        (datetime(2015, 9, 1, 7, 14, 59, tzinfo=pytz.UTC), ['daily']) ==
        schedule.next(clock.now(), 1, archive))


def test_do_not_expire_if_less_than_keep_and_inside_keep_interval(
        schedule, archive, clock):

    history = archive.history

    def add_revision(timestamp):
        revision = mock.Mock()
        revision.tags = ['daily']
        revision.timestamp = timestamp
        history.append(revision)
        history.sort(key=lambda x: x.timestamp)
        return revision

    clock.now.return_value = time_(2014, 5, 10, 10, 0)
    add_revision(time_(2014, 5, 10, 10, 0))
    schedule.expire(archive)
    assert not any(x.remove.called for x in history)

    add_revision(time_(2014, 5, 9, 10, 0))
    add_revision(time_(2014, 5, 8, 10, 0))
    add_revision(time_(2014, 5, 7, 10, 0))
    add_revision(time_(2014, 5, 6, 10, 0))
    schedule.expire(archive)
    assert not any(x.remove.called for x in history)
    # This is the one revision more than the basic 'keep' parameter
    # but its still within the keep*interval frame so we keep it.
    add_revision(time_(2014, 5, 6, 11, 0))
    assert [] == schedule.expire(archive)
    assert not any(x.remove.called for x in history)
    # This revision is more than keep and also outside the interval.
    # It gets removed.
    r = add_revision(time_(2014, 5, 4, 11, 0))
    schedule.expire(archive)
    assert r.remove.called
    assert sum(1 for x in history if x.remove.called) == 1
