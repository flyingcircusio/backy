from backy.backup import Archive
from backy.schedule import parse_duration, Schedule, next_in_interval
import backy.utils
from datetime import datetime, timedelta
import mock
import pytest
import pytz


def test_parse_duration():
    assert parse_duration('1d') == timedelta(seconds=24 * 60 * 60)
    assert parse_duration('2d') == timedelta(seconds=2 * 24 * 60 * 60)
    assert parse_duration('1h') == timedelta(seconds=60 * 60)
    assert parse_duration('3h') == timedelta(seconds=3 * 60 * 60)
    assert parse_duration('10m') == timedelta(seconds=10 * 60)
    assert parse_duration('25m') == timedelta(seconds=25 * 60)
    assert parse_duration('10s') == timedelta(seconds=10)
    assert parse_duration('5') == timedelta(seconds=5)


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


def test_first_backup_catches_up_all_tags_immediately(
        schedule, archive, clock):
    schedule.configure({'daily': {'interval': '1d', 'keep': 5},
                        'test': {'interval': '7d', 'keep': 7}})
    assert (
        (datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), {'daily', 'test'}) ==
        schedule.next(backy.utils.now(), 1, archive))


def test_tag_first_interval_after_now(schedule, archive, clock):
    assert ((datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_ideal(backy.utils.now(), 1))


def test_tag_second_interval_after_now(schedule, archive, clock):
    assert ((datetime(2015, 9, 3, 0, 0, 5, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_ideal(backy.utils.now() +
                                 timedelta(seconds=24 * 60 * 60),
                                 5))


def test_tag_second_interval_with_different_spread(schedule, archive, clock):
    assert ((datetime(2015, 9, 3, 0, 0, 5, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_ideal(backy.utils.now() +
                                 timedelta(seconds=25 * 60 * 60),
                                 5))


def test_tag_catchup_not_needed_for_recent(schedule, archive, clock):
    # A recent backup does not cause catchup to be triggered.
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=15)
    revision.tags = {'daily'}
    archive.history.append(revision)
    assert ((datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), set()) ==
            schedule._next_catchup(clock.now(), 1, archive))
    # This in turn causes the main next() function to return the regular next
    # interval.

    assert (
        (datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily'}) ==
        schedule.next(clock.now(), 1, archive))


def test_tag_catchup_not_needed_for_very_old(schedule, archive, clock):
    # If a backup has been overdue for too long, we expect the
    # tag to be scheduled soon anyway and we do not catch up to avoid
    # overload issues.
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=(24 * 60 * 60) * 1.6)
    revision.tags = {'daily'}
    archive.history.append(revision)
    assert ((datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), set()) ==
            schedule._next_catchup(clock.now(), 1, archive))
    # This in turn causes the main next() function to return the regular next
    # interval.
    assert (
        (datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily'}) ==
        schedule.next(clock.now(), 1, archive))


def test_tag_catchup_needed_for_recently_missed(schedule, archive, clock):
    revision = mock.Mock()
    # A catchup is needed because the daily backup has been longer than 1 day
    # but less than 1.5 days.
    revision.timestamp = clock.now() - timedelta(seconds=(24 * 60 * 60) * 1.2)
    revision.tags = {'daily'}
    archive.history.append(revision)
    assert ((datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_catchup(clock.now(), 1, archive))
    # This in turn causes the main next() function to also
    # return this date.
    assert (
        (datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), {'daily'}) ==
        schedule.next(clock.now(), 1, archive))


def test_do_not_expire_if_less_than_keep_and_inside_keep_interval(
        schedule, archive, clock):

    history = archive.history

    def add_revision(timestamp):
        revision = mock.Mock()
        revision.tags = {'daily'}
        revision.timestamp = timestamp
        history.append(revision)
        history.sort(key=lambda x: x.timestamp)
        return revision

    clock.now.return_value = datetime(2014, 5, 10, 10, 0)
    add_revision(datetime(2014, 5, 10, 10, 0))
    schedule.expire(archive)
    assert not any(x.remove.called for x in history)

    add_revision(datetime(2014, 5, 9, 10, 0))
    add_revision(datetime(2014, 5, 8, 10, 0))
    add_revision(datetime(2014, 5, 7, 10, 0))
    add_revision(datetime(2014, 5, 6, 10, 0))
    schedule.expire(archive)
    assert not any(x.remove.called for x in history)
    # This is the one revision more than the basic 'keep' parameter
    # but its still within the keep*interval frame so we keep it.
    add_revision(datetime(2014, 5, 6, 11, 0))
    assert [] == schedule.expire(archive)
    assert not any(x.remove.called for x in history)
    # This revision is more than keep and also outside the interval.
    # It gets removed.
    r = add_revision(datetime(2014, 5, 4, 11, 0))
    schedule.expire(archive)
    assert r.remove.called
    assert sum(1 for x in history if x.remove.called) == 1


def test_next_in_interval(clock):
    now = backy.utils.now()
    interval = timedelta(days=1)
    assert datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC) == now
    assert (
        datetime(2015, 9, 2, 0, 1, 40, tzinfo=pytz.UTC) ==
        next_in_interval(now, interval, 100))

    # t+30 minutes -> same interval
    now = now + timedelta(seconds=60 * 30)
    assert (
        datetime(2015, 9, 2, 0, 1, 40, tzinfo=pytz.UTC) ==
        next_in_interval(now, interval, 100))

    # t+60 minutes -> same interval
    now = now + timedelta(seconds=60 * 30)
    assert (
        datetime(2015, 9, 2, 0, 1, 40, tzinfo=pytz.UTC) ==
        next_in_interval(now, interval, 100))

    # t2 - a few seconds -> same interval
    now = datetime(2015, 9, 2, 0, 1, 20, tzinfo=pytz.UTC)
    assert (
        datetime(2015, 9, 2, 0, 1, 40, tzinfo=pytz.UTC) ==
        next_in_interval(now, interval, 100))

    # t2 + 1 second -> next interval
    now = datetime(2015, 9, 2, 0, 1, 41, tzinfo=pytz.UTC)
    assert (
        datetime(2015, 9, 3, 0, 1, 40, tzinfo=pytz.UTC) ==
        next_in_interval(now, interval, 100))
