import backy.backup
import backy.main
import datetime
import mock
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
