from backy.schedule import parse_duration, Schedule
import mock
import pytest
import time
import datetime


def time_(*args):
    dt = datetime.datetime(*args)
    return time.mktime(dt.timetuple())


def test_parse_duration():
    assert parse_duration('1d') == 24*60*60
    assert parse_duration('2d') == 2*24*60*60
    assert parse_duration('1h') == 60*60
    assert parse_duration('3h') == 3*60*60
    assert parse_duration('10m') == 10*60
    assert parse_duration('25m') == 25*60
    assert parse_duration('10s') == 10
    assert parse_duration('5') == 5


@pytest.fixture
def schedule():
    backup = mock.Mock()
    backup.config = {'schedule': {'daily': {'interval': '1d', 'keep': 5}}}
    return Schedule(backup)


def test_schedule_object(schedule):
    backup = mock.Mock()
    backup.config = {'schedule': {'daily': {'interval': '1d', 'keep': 5}}}
    schedule = Schedule(backup)
    assert schedule.schedule == backup.config['schedule']


def test_tag_is_due_if_never_done_before(schedule):
    schedule.backup.find_revisions.return_value = []
    assert set(['daily']) == schedule.next_due()


def test_tag_not_due_if_within_interval(schedule):
    revision = mock.Mock()
    revision.timestamp = time.time()
    schedule.backup.now.return_value = revision.timestamp + 60
    schedule.backup.find_revisions.return_value = [revision]
    assert set() == schedule.next_due()


def test_tag_due_if_after_interval(schedule):
    revision = mock.Mock()
    revision.timestamp = time.time()
    schedule.backup.now.return_value = revision.timestamp + 25*60*60
    schedule.backup.find_revisions.return_value = [revision]
    assert set(['daily']) == schedule.next_due()


def test_do_not_expire_if_less_than_keep_and_inside_keep_interval(schedule):
    history = []
    schedule.backup.find_revisions.return_value = history
    schedule.backup.revision_history = history

    def add_revision(timestamp):
        revision = mock.Mock()
        revision.tags = ['daily']
        revision.timestamp = timestamp
        history.append(revision)
        history.sort(key=lambda x: x.timestamp)
        return revision

    schedule.backup.now.return_value = time_(2014, 5, 10, 10, 0)
    add_revision(time_(2014, 5, 10, 10, 0))
    schedule.expire()
    assert not any(x.remove.called for x in history)

    add_revision(time_(2014, 5, 9, 10, 0))
    add_revision(time_(2014, 5, 8, 10, 0))
    add_revision(time_(2014, 5, 7, 10, 0))
    add_revision(time_(2014, 5, 6, 10, 0))
    schedule.expire()
    assert not any(x.remove.called for x in history)
    # This is the one revision more than the basic 'keep' parameter
    # but its still within the keep*interval frame so we keep it.
    add_revision(time_(2014, 5, 6, 11, 0))
    schedule.expire()
    assert not any(x.remove.called for x in history)
    # This revision is more than keep and also outside the interval.
    # It gets removed.
    r = add_revision(time_(2014, 5, 4, 11, 0))
    schedule.expire()
    assert r.remove.called
    assert sum(1 for x in history if x.remove.called) == 1
