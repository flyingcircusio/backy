import difflib


def pytest_assertrepr_compare(op, left, right):
    if left.__class__.__name__ != 'Ellipsis':
        return

    return list(difflib.ndiff(left.ellipsis.split('\n'), right.split('\n')))
