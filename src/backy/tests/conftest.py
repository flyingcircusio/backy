import backy.backup
import backy.main
import pytest
import mock


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
    clock.now.return_value = 1441091207.0
    monkeypatch.setattr(backy.utils, 'now', clock.now)
    return clock
