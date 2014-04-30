import backy.main
import difflib
import pytest


def pytest_assertrepr_compare(op, left, right):
    if left.__class__.__name__ != 'Ellipsis':
        return

    return [''] + list(
        difflib.ndiff(left.ellipsis.split('\n'), right.split('\n')))


@pytest.fixture(autouse=True)
def wrap_logging(monkeypatch):
    monkeypatch.setattr(backy.main, 'init_logging', lambda verbose: None)
