from backy.tests import Ellipsis
from backy.utils import files_are_equal
import backy.backup
import os
import pytest


def test_format_timestamp():
    assert '1970-01-01 01:00:00' == backy.backup.format_timestamp(0)


def test_ellipsis():
    assert Ellipsis('...') == 'asdf'
    assert Ellipsis('a...c') == 'abc'
    assert Ellipsis('a...d') != 'abc'
    assert Ellipsis('a...c...g') == 'abcdefg'
    assert not Ellipsis('') == 'asdf'
    with pytest.raises(Exception):
        assert Ellipsis('') == 'abcdefg'


def test_ellipsis_lines():
    assert Ellipsis("""
asdf...bsdf
csdf
...
dsdf...fooo
""") == """
asdffoobarbsdf
csdf
gnar gnarr gnarr
dsdfblablafooo
"""


def test_ellipsis_report():
    report = Ellipsis("""
asdf...bsdf
csdf
...
dsdf...fooo
""").compare("""
asdffoobarbsdf
csdf
gnar gnar gnarr
dsdfblablafooobar
""")
    assert not report.matches
    assert """\
  asdffoobarbsdf
  csdf
  gnar gnar gnarr
  dsdfblablafooobar
- dsdf...fooo
- \
""" == '\n'.join(report.diff)


def test_ellipsis_escaping():
    obj = (object(),)
    assert Ellipsis('(<object object at ...>,)') == repr(obj)


def test_compare_files_same(tmpdir):
    os.chdir(str(tmpdir))
    with open('a', 'wb') as f:
        f.write(b'asdf')
    with open('b', 'wb') as f:
        f.write(b'asdf')

    assert files_are_equal(open('a', 'rb'), open('b', 'rb'))


def test_compare_files_different_content(tmpdir):
    os.chdir(str(tmpdir))
    with open('a', 'wb') as f:
        f.write(b'asdf')
    with open('b', 'wb') as f:
        f.write(b'bsdf')

    assert not files_are_equal(open('a', 'rb'), open('b', 'rb'))


def test_compare_files_different_length(tmpdir):
    os.chdir(str(tmpdir))
    with open('a', 'wb') as f:
        f.write(b'asdf1')
    with open('b', 'wb') as f:
        f.write(b'bsdf')

    assert not files_are_equal(open('a', 'rb'), open('b', 'rb'))
