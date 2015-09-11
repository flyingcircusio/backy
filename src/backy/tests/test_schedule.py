from backy.revision import Revision
from backy.schedule import parse_duration, next_in_interval
from datetime import datetime, timedelta
import backy.utils
from unittest import mock
import os.path
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


def test_tag_catchup_does_not_stumble_on_adhoc_tags_in_archive(
        schedule, archive, clock):
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=15)
    revision.tags = {'test'}
    archive.history.append(revision)
    schedule._next_catchup(clock.now(), 1, archive)


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
        revision = Revision(len(archive.history) + 1, archive)
        revision.materialize()
        revision.tags = {'daily'}
        revision.timestamp = timestamp
        history.append(revision)
        history.sort(key=lambda x: x.timestamp)
        return revision

    clock.now.return_value = datetime(2014, 5, 10, 10, 0)
    add_revision(datetime(2014, 5, 10, 10, 0))
    assert [] == schedule.expire(archive)
    archive.scan()
    assert len(archive.history) == 1
    assert archive.history[0].tags == {'daily'}

    add_revision(datetime(2014, 5, 9, 10, 0))
    add_revision(datetime(2014, 5, 8, 10, 0))
    add_revision(datetime(2014, 5, 7, 10, 0))
    add_revision(datetime(2014, 5, 6, 10, 0))
    assert [] == schedule.expire(archive)
    archive.scan()
    assert len(archive.history) == 5
    assert [{'daily'}] * 5 == [r.tags for r in history]

    # This is the one revision more than the basic 'keep' parameter
    # but its still within the keep*interval frame so we keep it.
    add_revision(datetime(2014, 5, 6, 11, 0))
    assert [] == schedule.expire(archive)
    assert [{'daily'}] * 6 == [r.tags for r in history]

    # This revision is more than keep and also outside the interval.
    # It gets its tag removed and disappears.
    r = add_revision(datetime(2014, 5, 4, 11, 0))
    assert os.path.exists(r.filename)
    assert [r] == schedule.expire(archive)
    archive.scan()
    assert [{'daily'}] * 6 == [rev.tags for rev in history]
    assert not os.path.exists(r.filename)

    # If we have unknown tags, then those do not expire. However, the
    # known tag disappears but then the file remains because there's still
    # a tag left.
    r = add_revision(datetime(2014, 5, 4, 11, 0))
    r.tags = {'daily', 'test'}
    assert os.path.exists(r.filename)
    assert [] == schedule.expire(archive)
    archive.scan()
    assert [{'test'}] + [{'daily'}] * 6 == [rev.tags for rev in history]
    assert os.path.exists(r.filename)


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
