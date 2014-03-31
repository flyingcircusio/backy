from backy.tests import Ellipsis
import backy.backup
import pytest


def test_format_timestamp():
    assert '1970-01-01 01:00:00' == backy.backup.format_timestamp(0)


def test_ellipsis():
    assert Ellipsis('...') == 'asdf'
    assert Ellipsis('a...c') == 'abc'
    assert Ellipsis('a...d') != 'abc'
    assert Ellipsis('a...c...g') == 'abcdefg'
    with pytest.raises(Exception):
        assert Ellipsis('') == 'abcdefg'
