from unittest import mock
import backy.backup
import backy.main
import backy.schedule
import datetime
import os
import pytest
import pytz
import structlog
import shutil


fixtures = os.path.dirname(__file__) + '/tests/samples'


@pytest.fixture
def simple_file_config(tmpdir, monkeypatch):
    shutil.copy(fixtures + '/simple_file/config', str(tmpdir))
    monkeypatch.chdir(tmpdir)
    b = backy.backup.Backup(str(tmpdir))
    return b


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
def backup(schedule, tmpdir):
    with open(str(tmpdir / 'config'), 'wb') as f:
        f.write(b"{'type': 'file', 'filename': 'test'}")
    return backy.backup.Backup(str(tmpdir))


@pytest.fixture(scope='session')
def setup_structlog():
    from . import utils
    utils.log_data = []
    log_exceptions = False  # set to True to get detailed tracebacks

    def test_logger(logger, method_name, event):
        result = []

        if log_exceptions:
            stack = event.pop("stack", None)
            exc = event.pop("exception", None)
        for key in sorted(event):
            result.append('{}={}'.format(key, event[key]))
        utils.log_data.append(' '.join(result))
        if log_exceptions:
            if stack:
                utils.log_data.extend(stack.splitlines())
            if exc:
                utils.log_data.extend(exc.splitlines())
        raise structlog.DropEvent

    structlog.configure(processors=(
        [structlog.processors.format_exc_info] if log_exceptions else [] +
        [test_logger]))


@pytest.fixture(autouse=True)
def reset_structlog(setup_structlog):
    from . import utils
    utils.log_data = []
