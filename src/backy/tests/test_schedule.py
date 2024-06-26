from datetime import datetime, timedelta, timezone
from unittest import mock

import backy.utils
from backy.revision import Revision
from backy.schedule import Schedule, next_in_interval, parse_duration

UTC = timezone.utc


def test_parse_duration():
    assert parse_duration("1w") == timedelta(seconds=24 * 60 * 60 * 7)
    assert parse_duration("1d") == timedelta(seconds=24 * 60 * 60)
    assert parse_duration("2d") == timedelta(seconds=2 * 24 * 60 * 60)
    assert parse_duration("1h") == timedelta(seconds=60 * 60)
    assert parse_duration("3h") == timedelta(seconds=3 * 60 * 60)
    assert parse_duration("10m") == timedelta(seconds=10 * 60)
    assert parse_duration("25m") == timedelta(seconds=25 * 60)
    assert parse_duration("10s") == timedelta(seconds=10)
    assert parse_duration("5") == timedelta(seconds=5)


def test_first_backup_catches_up_all_tags_immediately_in_next_interval(
    schedule, repository, clock
):
    schedule.configure(
        {
            "daily": {"interval": "1d", "keep": 5},
            "test": {"interval": "7d", "keep": 7},
        }
    )
    assert (
        datetime(2015, 9, 2, 0, 0, 1, tzinfo=UTC),
        {"daily", "test"},
    ) == schedule.next(backy.utils.now(), 1, repository)


def test_tag_first_interval_after_now(schedule, repository, clock):
    assert (
        datetime(2015, 9, 2, 0, 0, 1, tzinfo=UTC),
        {"daily"},
    ) == schedule._next_ideal(backy.utils.now(), 1)


def test_tag_second_interval_after_now(schedule, repository, clock):
    assert (
        datetime(2015, 9, 3, 0, 0, 5, tzinfo=UTC),
        {"daily"},
    ) == schedule._next_ideal(
        backy.utils.now() + timedelta(seconds=24 * 60 * 60), 5
    )


def test_tag_second_interval_with_different_spread(schedule, repository, clock):
    assert (
        datetime(2015, 9, 3, 0, 0, 5, tzinfo=UTC),
        {"daily"},
    ) == schedule._next_ideal(
        backy.utils.now() + timedelta(seconds=25 * 60 * 60), 5
    )


def test_tag_catchup_not_needed_for_recent(schedule, repository, clock):
    # A recent backup does not cause catchup to be triggered.
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=15)
    revision.tags = {"daily"}
    revision.stats = {"duration": 10}
    repository.history.append(revision)
    assert set() == schedule._missed(repository)
    # This in turn causes the main next() function to return the regular next
    # interval.
    assert (
        datetime(2015, 9, 2, 0, 0, 1, tzinfo=UTC),
        {"daily"},
    ) == schedule.next(clock.now(), 1, repository)


def test_tag_catchup_does_not_stumble_on_adhoc_tags_in_backup(
    schedule, repository, clock
):
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=15)
    revision.tags = {"test"}
    revision.stats = {"duration": 10}
    repository.history.append(revision)
    assert {"daily"} == schedule._missed(repository)


def test_tag_catchup_until_5_minutes_before_next(schedule, repository, clock):
    # If a backup has been overdue for too long, we expect the
    # tag to be scheduled soon anyway and we do not catch up to avoid
    # overload issues.
    revision = mock.Mock()
    revision.timestamp = clock.now() - timedelta(seconds=(24 * 60 * 60) * 1.6)
    revision.tags = {"daily"}
    revision.stats = {"duration": 10}
    revision.write_info()
    repository.history.append(revision)
    assert {"daily"} == schedule._missed(repository)
    # This in turn causes the main next() function to return the regular next
    # interval.
    assert (
        datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC),
        {"daily"},
    ) == schedule.next(clock.now(), 1, repository)
    # As we approach the 5 minute mark before the next regular interval,
    # we then flip towards the ideal time.
    clock.now.return_value = datetime(2015, 9, 1, 23, 55, 0, tzinfo=UTC)
    assert (clock.now(), {"daily"}) == schedule.next(
        datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC), 1, repository
    )

    clock.now.return_value = datetime(2015, 9, 1, 23, 55, 1, tzinfo=UTC)
    assert (
        datetime(2015, 9, 2, 0, 0, 1, tzinfo=UTC),
        {"daily"},
    ) == schedule.next(
        datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC), 1, repository
    )


def test_tag_catchup_needed_for_recently_missed(repository, clock):
    revision = mock.Mock()

    schedule = backy.schedule.Schedule()
    schedule.configure(
        {
            "daily": {"interval": "1d", "keep": 5},
            "hourly": {"interval": "1h", "keep": 24},
            "weekly": {"interval": "7d", "keep": 7},
        }
    )

    # A bad situation that we want to remedy as fast as possible:
    #   - no weekly backup at all
    #   - no hourly backup at all
    #   - the only daily is older than 1.2 times
    revision.timestamp = clock.now() - timedelta(seconds=(24 * 60 * 60) * 1.2)
    revision.tags = {"daily"}
    revision.stats = {"duration": 10}
    repository.history.append(revision)

    assert {"daily", "weekly", "hourly"} == schedule._missed(repository)
    # This in turn causes the main next() function to also
    # return this date.
    assert (
        datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC),
        {"daily", "weekly", "hourly"},
    ) == schedule.next(clock.now(), 1, repository)


def test_do_not_expire_if_less_than_keep_and_inside_keep_interval(
    schedule, repository, clock, log
):
    def add_revision(timestamp):
        revision = Revision.create(repository, {"daily"}, log)
        revision.uuid = str(len(repository.history) + 1)
        revision.timestamp = timestamp
        revision.materialize()
        repository.history.append(revision)
        repository.history.sort(key=lambda x: x.timestamp)
        return revision

    clock.now.return_value = datetime(2014, 5, 10, 10, 0, tzinfo=UTC)
    add_revision(datetime(2014, 5, 10, 10, 0, tzinfo=UTC))
    assert [] == schedule.expire(repository)
    repository.scan()
    assert len(repository.history) == 1
    assert repository.history[0].tags == {"daily"}

    add_revision(datetime(2014, 5, 9, 10, 0, tzinfo=UTC))
    add_revision(datetime(2014, 5, 8, 10, 0, tzinfo=UTC))
    add_revision(datetime(2014, 5, 7, 10, 0, tzinfo=UTC))
    add_revision(datetime(2014, 5, 6, 10, 0, tzinfo=UTC))
    assert [] == schedule.expire(repository)
    repository.scan()
    assert len(repository.history) == 5
    assert [{"daily"}] * 5 == [r.tags for r in repository.history]

    # This is the one revision more than the basic 'keep' parameter
    # but its still within the keep*interval frame so we keep it.
    add_revision(datetime(2014, 5, 6, 11, 0, tzinfo=UTC))
    assert [] == schedule.expire(repository)
    assert [{"daily"}] * 6 == [r.tags for r in repository.history]

    # This revision is more than keep and also outside the interval.
    # It gets its tag removed and disappears.
    r = add_revision(datetime(2014, 5, 4, 11, 0, tzinfo=UTC))
    assert r.filename.with_suffix(".rev").exists()
    removed = [x for x in schedule.expire(repository)]
    assert [r.uuid] == [x.uuid for x in removed]
    repository.scan()
    assert [{"daily"}] * 6 == [rev.tags for rev in repository.history]
    assert not r.filename.with_suffix(".rev").exists()

    # If we have manual tags, then those do not expire. However, the
    # known and unknown tag disappear but then the file remains
    # because there's still a manual tag left.
    r = add_revision(datetime(2014, 5, 4, 11, 0, tzinfo=UTC))
    r.tags = {"daily", "manual:test", "unknown"}
    r.write_info()
    assert r.filename.with_suffix(".rev").exists()
    expired = schedule.expire(repository)
    assert [] == [x.uuid for x in expired]
    repository.scan()
    assert [{"manual:test"}] + [{"daily"}] * 6 == [
        rev.tags for rev in repository.history
    ]
    assert r.filename.with_suffix(".rev").exists()


def test_next_in_interval(clock):
    now = backy.utils.now()
    interval = timedelta(days=1)
    assert datetime(2015, 9, 1, 7, 6, 47, tzinfo=UTC) == now
    assert datetime(2015, 9, 2, 0, 1, 40, tzinfo=UTC) == next_in_interval(
        now, interval, 100
    )

    # t+30 minutes -> same interval
    now = now + timedelta(seconds=60 * 30)
    assert datetime(2015, 9, 2, 0, 1, 40, tzinfo=UTC) == next_in_interval(
        now, interval, 100
    )

    # t+60 minutes -> same interval
    now = now + timedelta(seconds=60 * 30)
    assert datetime(2015, 9, 2, 0, 1, 40, tzinfo=UTC) == next_in_interval(
        now, interval, 100
    )

    # t2 - a few seconds -> same interval
    now = datetime(2015, 9, 2, 0, 1, 20, tzinfo=UTC)
    assert datetime(2015, 9, 2, 0, 1, 40, tzinfo=UTC) == next_in_interval(
        now, interval, 100
    )

    # t2 + 1 second -> next interval
    now = datetime(2015, 9, 2, 0, 1, 41, tzinfo=UTC)
    assert datetime(2015, 9, 3, 0, 1, 40, tzinfo=UTC) == next_in_interval(
        now, interval, 100
    )


def test_sorted_tags():
    s = Schedule()
    s.configure(
        {
            "daily": {"interval": "1d", "keep": 5},
            "weekly": {"interval": "1w", "keep": 4},
            "monthly": {"interval": "4w", "keep": 3},
        }
    )
    assert ["daily", "weekly", "monthly"] == list(
        s.sorted_tags(s.schedule.keys())
    )
