from unittest import mock
import backy.backup
import backy.main
import backy.schedule
import datetime
import os
import pytest
import pytz


def pytest_assertrepr_compare(op, left, right):
    if left.__class__.__name__ != 'Ellipsis':
        return

    report = left.compare(right)
    return report.diff


@pytest.fixture(autouse=True)
def wrap_logging(monkeypatch):
    monkeypatch.setattr(backy.main, 'init_logging',
                        lambda backupdir, verbose: None)


@pytest.yield_fixture(autouse=True)
def fix_cwd():
    cwd = os.getcwd()
    yield
    os.chdir(cwd)


@pytest.fixture
def clock(monkeypatch):
    class Clock(object):
        now = mock.Mock()
    clock = Clock()
    # 2015-09-01 ~09:15
    clock.now.return_value = datetime.datetime(
        2015, 9, 1, 7, 6, 47, tzinfo=pytz.UTC)
    monkeypatch.setattr(backy.utils, 'now', clock.now)
    return clock


@pytest.fixture
def schedule():
    schedule = backy.schedule.Schedule()
    schedule.configure({'daily': {'interval': '1d', 'keep': 5}})
    return schedule


@pytest.fixture
def archive(tmpdir):
    a = backy.backup.Archive(str(tmpdir))
    a.scan = lambda: None
    return a


@pytest.fixture
def backup(schedule, tmpdir):
    return backy.backup.Backup(str(tmpdir))


@pytest.fixture
def forget_about_btrfs(monkeypatch):
    monkeypatch.setenv('BACKY_FORGET_ABOUT_BTRFS', '1')
