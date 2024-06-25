import datetime
import os
import random
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
import structlog

import backy.backup
import backy.logging
import backy.schedule
from backy import utils


@pytest.fixture(autouse=True, scope="session")
def fix_pytest_coverage_465():
    if "COV_CORE_SOURCE" in os.environ:
        os.environ["COV_CORE_SOURCE"] = os.path.abspath(
            os.environ["COV_CORE_SOURCE"]
        )


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
def seed_random(monkeypatch):
    random.seed(0)


@pytest.fixture
def schedule():
    schedule = backy.schedule.Schedule()
    schedule.configure({"daily": {"interval": "1d", "keep": 5}})
    return schedule


@pytest.fixture(scope="session")
def setup_structlog():
    utils.log_data = ""

    class PytestLogger:
        def msg(self, message: str):
            utils.log_data += message + "\n"

    backy.logging.init_logging(True, defaults={"taskid": "AAAA"})
    structlog.get_config()["logger_factory"].factories["file"] = PytestLogger


@pytest.fixture(autouse=True)
def reset_structlog(setup_structlog):
    utils.log_data = ""
