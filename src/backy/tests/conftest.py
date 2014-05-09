import backy.backup
import backy.main
import pytest
import time


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
def mocked_now(monkeypatch):
    class Clock(object):
        now = None
    clock = Clock()
    clock.now = time.time()
    monkeypatch.setattr(backy.backup.Backup, 'now', lambda self: clock.now)
    return clock
