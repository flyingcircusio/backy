from backy.revision import Revision
from backy.schedule import parse_duration, next_in_interval, Schedule
from datetime import datetime, timedelta
from unittest import mock
import backy.utils
import os.path
import pytz


def test_parse_duration():
    assert parse_duration('1w') == timedelta(seconds=24 * 60 * 60 * 7)
    assert parse_duration('1d') == timedelta(seconds=24 * 60 * 60)
    assert parse_duration('2d') == timedelta(seconds=2 * 24 * 60 * 60)
    assert parse_duration('1h') == timedelta(seconds=60 * 60)
    assert parse_duration('3h') == timedelta(seconds=3 * 60 * 60)
    assert parse_duration('10m') == timedelta(seconds=10 * 60)
    assert parse_duration('25m') == timedelta(seconds=25 * 60)
    assert parse_duration('10s') == timedelta(seconds=10)
    assert parse_duration('5') == timedelta(seconds=5)


def test_first_backup_catches_up_all_tags_immediately_in_next_interval(
        schedule, backup, clock):
    schedule.configure({'daily': {'interval': '1d', 'keep': 5},
                        'test': {'interval': '7d', 'keep': 7}})
    assert (
        (datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily', 'test'}) ==
        schedule.next(backy.utils.now(), 1, backup))


def test_tag_first_interval_after_now(schedule, backup, clock):
    assert ((datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_ideal(backy.utils.now(), 1))


def test_tag_second_interval_after_now(schedule, backup, clock):
    assert ((datetime(2015, 9, 3, 0, 0, 5, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_ideal(backy.utils.now() +
                                 timedelta(seconds=24 * 60 * 60),
                                 5))


def test_tag_second_interval_with_different_spread(schedule, backup, clock):
    assert ((datetime(2015, 9, 3, 0, 0, 5, tzinfo=pytz.UTC), {'daily'}) ==
            schedule._next_ideal(backy.utils.now() +
                                 timedelta(seconds=25 * 60 * 60),
                                 5))


def test_tag_catchup_not_needed_for_recent(schedule, backup, clock):
    # A recent backup does not cause catchup to be triggered.
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=15)
    revision.tags = {'daily'}
    backup.history.append(revision)
    assert set() == schedule._missed(backup)
    # This in turn causes the main next() function to return the regular next
    # interval.
    assert (
        (datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily'}) ==
        schedule.next(clock.now(), 1, backup))


def test_tag_catchup_does_not_stumble_on_adhoc_tags_in_backup(
        schedule, backup, clock):
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=15)
    revision.tags = {'test'}
    backup.history.append(revision)
    assert set({'daily'}) == schedule._missed(backup)


def test_tag_catchup_until_5_minutes_before_next(schedule, backup, clock):
    # If a backup has been overdue for too long, we expect the
    # tag to be scheduled soon anyway and we do not catch up to avoid
    # overload issues.
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=(24 * 60 * 60) * 1.6)
    revision.tags = {'daily'}
    revision.write_info()
    backup.history.append(revision)
    assert set({'daily'}) == schedule._missed(backup)
    # This in turn causes the main next() function to return the regular next
    # interval.
    assert (
        (datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), {'daily'}) ==
        schedule.next(clock.now(), 1, backup))
    # As we approach the 5 minute mark before the next regular interval,
    # we then flip towards the ideal time.
    clock.now.return_value = datetime(2015, 9, 1, 23, 55, 0, tzinfo=pytz.UTC)
    assert (
        (clock.now(), {'daily'}) == schedule.next(
            datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), 1, backup))

    clock.now.return_value = datetime(2015, 9, 1, 23, 55, 1, tzinfo=pytz.UTC)
    assert (
        (datetime(2015, 9, 2, 0, 0, 1, tzinfo=pytz.UTC), {'daily'}) ==
        schedule.next(
            datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC), 1, backup))


def test_tag_catchup_needed_for_recently_missed(backup, clock):
    revision = mock.Mock()

    schedule = backy.schedule.Schedule()
    schedule.configure({'daily': {'interval': '1d', 'keep': 5},
                        'hourly': {'interval': '1h', 'keep': 24},
                        'weekly': {'interval': '7d', 'keep': 7}})

    # A bad situation that we want to remedy as fast as possible:
    #   - no weekly backup at all
    #   - no hourly backup at all
    #   - the only daily is older than 1.2 times
    revision.timestamp = clock.now() - timedelta(seconds=(24 * 60 * 60) * 1.2)
    revision.tags = {'daily'}
    backup.history.append(revision)

    assert {'daily', 'weekly', 'hourly'} == schedule._missed(backup)
    # This in turn causes the main next() function to also
    # return this date.
    assert (
        (datetime(2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC),
         {'daily', 'weekly', 'hourly'}) ==
        schedule.next(clock.now(), 1, backup))


def test_do_not_expire_if_less_than_keep_and_inside_keep_interval(
        schedule, backup, clock):
    def add_revision(timestamp):
        revision = Revision(
            backup, len(backup.history) + 1, timestamp=timestamp)
        revision.tags = {'daily'}
        revision.materialize()
        backup.history.append(revision)
        backup.history.sort(key=lambda x: x.timestamp)
        return revision

    clock.now.return_value = datetime(2014, 5, 10, 10, 0, tzinfo=pytz.UTC)
    add_revision(datetime(2014, 5, 10, 10, 0, tzinfo=pytz.UTC))
    assert [] == schedule.expire(backup)
    backup.scan()
    assert len(backup.history) == 1
    assert backup.history[0].tags == {'daily'}

    add_revision(datetime(2014, 5, 9, 10, 0, tzinfo=pytz.UTC))
    add_revision(datetime(2014, 5, 8, 10, 0, tzinfo=pytz.UTC))
    add_revision(datetime(2014, 5, 7, 10, 0, tzinfo=pytz.UTC))
    add_revision(datetime(2014, 5, 6, 10, 0, tzinfo=pytz.UTC))
    assert [] == schedule.expire(backup)
    backup.scan()
    assert len(backup.history) == 5
    assert [{'daily'}] * 5 == [r.tags for r in backup.history]

    # This is the one revision more than the basic 'keep' parameter
    # but its still within the keep*interval frame so we keep it.
    add_revision(datetime(2014, 5, 6, 11, 0, tzinfo=pytz.UTC))
    assert [] == schedule.expire(backup)
    assert [{'daily'}] * 6 == [r.tags for r in backup.history]

    # This revision is more than keep and also outside the interval.
    # It gets its tag removed and disappears.
    r = add_revision(datetime(2014, 5, 4, 11, 0, tzinfo=pytz.UTC))
    assert os.path.exists(r.filename + '.rev')
    removed = [x for x in schedule.expire(backup)]
    assert [r.uuid] == [x.uuid for x in removed]
    backup.scan()
    assert [{'daily'}] * 6 == [rev.tags for rev in backup.history]
    assert not os.path.exists(r.filename + '.rev')

    # If we have unknown tags, then those do not expire. However, the
    # known tag disappears but then the file remains because there's still
    # a tag left.
    r = add_revision(datetime(2014, 5, 4, 11, 0, tzinfo=pytz.UTC))
    r.tags = {'daily', 'test'}
    r.write_info()
    assert os.path.exists(r.filename + '.rev')
    expired = schedule.expire(backup)
    assert [] == [x.uuid for x in expired]
    backup.scan()
    assert [{'test'}] + [{'daily'}] * 6 == [rev.tags for rev in backup.history]
    assert os.path.exists(r.filename + '.rev')


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


def test_sorted_tags():
    s = Schedule()
    s.configure({'daily': {'interval': '1d', 'keep': 5},
                 'weekly': {'interval': '1w', 'keep': 4},
                 'monthly': {'interval': '4w', 'keep': 3},
                 })
    assert ['daily', 'weekly', 'monthly'] == list(
        s.sorted_tags(s.schedule.keys()))
