from backy.backup import Archive
from backy.schedule import parse_duration, Schedule
import backy.utils
import datetime
import mock
import pytest
import time


def time_(*args):
    dt = datetime.datetime(*args)
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
    return Archive(str(tmpdir))


def test_tag_not_due_if_within_interval(schedule, clock):
    assert ((datetime.datetime(2015, 9, 2, 1, 59, 59), ['daily']) ==
            schedule.next(backy.utils.now() + 60, 1))


def test_tag_due_if_after_interval(schedule, clock):
    assert ((datetime.datetime(2015, 9, 3, 1, 59, 59), ['daily']) ==
            schedule.next(backy.utils.now() + 25 * 60 * 60, 1))


def test_tag_due_if_after_interval_different_spread(schedule, clock):
    assert ((datetime.datetime(2015, 9, 3, 1, 59, 55), ['daily']) ==
            schedule.next(backy.utils.now() + 25 * 60 * 60, 5))


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
