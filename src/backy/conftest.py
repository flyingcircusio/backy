import datetime
import os
import shutil
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
import structlog

import backy.backup
import backy.logging
import backy.main
import backy.schedule
from backy import utils

fixtures = os.path.dirname(__file__) + "/tests/samples"


@pytest.fixture(autouse=True, scope="session")
def fix_pytest_coverage_465():
    if "COV_CORE_SOURCE" in os.environ:
        os.environ["COV_CORE_SOURCE"] = os.path.abspath(
            os.environ["COV_CORE_SOURCE"]
        )


@pytest.fixture
def simple_file_config(tmpdir, monkeypatch, log):
    shutil.copy(fixtures + "/simple_file/config", str(tmpdir))
    monkeypatch.chdir(tmpdir)
    b = backy.backup.Backup(str(tmpdir), log)
    return b


def pytest_assertrepr_compare(op, left, right):
    if left.__class__.__name__ != "Ellipsis":
        return

    report = left.compare(right)
    return report.diff


@pytest.fixture(autouse=True)
def log(monkeypatch):
    def noop_init_logging(*args, **kwargs):
        pass

    monkeypatch.setattr(backy.logging, "init_logging", noop_init_logging)
    return structlog.stdlib.get_logger()


@pytest.fixture(autouse=True)
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
        2015, 9, 1, 7, 6, 47, tzinfo=ZoneInfo("UTC")
    )
    monkeypatch.setattr(backy.utils, "now", clock.now)
    return clock


@pytest.fixture
def schedule():
    schedule = backy.schedule.Schedule()
    schedule.configure({"daily": {"interval": "1d", "keep": 5}})
    return schedule


@pytest.fixture
def backup(schedule, tmpdir, log):
    with open(str(tmpdir / "config"), "wb") as f:
        f.write(
            b"{'source': {'type': 'file', 'filename': 'test'},"
            b"'schedule': {'daily': {'interval': '1d', 'keep': 7}}}"
        )
    return backy.backup.Backup(str(tmpdir), log)


@pytest.fixture(scope="session")
def setup_structlog():
    utils.log_data = ""

    class PytestLogger:
        def msg(self, message: str):
            utils.log_data += message + "\n"

    backy.logging.init_logging(True)
    structlog.get_config()["logger_factory"].factories["file"] = PytestLogger
    yield structlog.get_config()["processors"][-1]


@pytest.fixture(autouse=True)
def reset_structlog(setup_structlog):
    utils.log_data = ""
    setup_structlog.default_job_name = ""
