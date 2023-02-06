import datetime
import os
import sys
from zoneinfo import ZoneInfo

import pytest

import backy.utils
from backy.tests import Ellipsis
from backy.utils import (
    SafeFile,
    copy_overwrite,
    files_are_equal,
    files_are_roughly_equal,
)


def test_ellipsis():
    assert Ellipsis("...") == "asdf"
    assert Ellipsis("a...c") == "abc"
    assert Ellipsis("a...d") != "abc"
    assert Ellipsis("a...c...g") == "abcdefg"
    assert (
        Ellipsis(
            """\
...
a
...
d...g
...
z
..."""
        )
        == """\
000
111
a
foobar
drinking
something
z
zoo"""
    )
    assert not Ellipsis("") == "asdf"
    with pytest.raises(Exception):
        assert Ellipsis("") == "abcdefg"


def test_ellipsis_lines():
    assert (
        Ellipsis(
            """
asdf...bsdf
csdf
...
dsdf...fooo
"""
        )
        == """
asdffoobarbsdf
csdf
gnar gnarr gnarr
dsdfblablafooo
"""
    )


def test_ellipsis_report():
    report = Ellipsis(
        """
asdf...bsdf
csdf
...
dsdf...fooo
"""
    ).compare(
        """
asdffoobarbsdf
csdf
gnar gnar gnarr
dsdfblablafooobar
"""
    )
    assert not report.matches
    assert """\
  asdffoobarbsdf
  csdf
  gnar gnar gnarr
  dsdfblablafooobar
- dsdf...fooo
- \
""" == "\n".join(
        report.diff
    )


def test_ellipsis_escaping():
    obj = (object(),)
    assert Ellipsis("(<object object at ...>,)") == repr(obj)


def test_compare_files_same(tmpdir):
    os.chdir(str(tmpdir))
    with open("a", "wb") as f:
        f.write(b"asdf")
    with open("b", "wb") as f:
        f.write(b"asdf")

    assert files_are_equal(open("a", "rb"), open("b", "rb"))


def test_compare_files_different_content(tmpdir):
    os.chdir(str(tmpdir))
    with open("a", "wb") as f:
        f.write(b"asdf")
    with open("b", "wb") as f:
        f.write(b"bsdf")

    assert not files_are_equal(open("a", "rb"), open("b", "rb"))


def test_compare_files_different_length(tmpdir):
    os.chdir(str(tmpdir))
    with open("a", "wb") as f:
        f.write(b"asdf1")
    with open("b", "wb") as f:
        f.write(b"bsdf")

    assert not files_are_equal(open("a", "rb"), open("b", "rb"))


def test_safe_writable_rename_no_writeprotect(tmpdir):
    os.chdir(str(tmpdir))
    with SafeFile("asdf") as f:
        f.open_new("wb")
        assert f.name != "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"


def test_safe_writable_no_rename_no_writeprotect(tmpdir):
    os.chdir(str(tmpdir))
    with SafeFile("asdf") as f:
        f.open_inplace("wb")
        assert f.name == "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"


def test_safe_writable_no_rename_no_writeprotect_existing_file(tmpdir):
    os.chdir(str(tmpdir))
    open("asdf", "wb").write(b"bsdf")
    with SafeFile("asdf") as f:
        f.open_inplace("r+b")
        assert f.read() == b"bsdf"
        f.seek(0)
        assert f.name == "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"


def test_safe_writable_rename_writeprotect(tmpdir):
    os.chdir(str(tmpdir))
    with SafeFile("asdf") as f:
        f.use_write_protection()
        f.open_new("wb")
        assert f.name != "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"

    with pytest.raises(IOError):
        open("asdf", "wb")


def test_safe_edit_noop(tmpdir):
    with SafeFile(str(tmpdir / "file")):
        pass
    assert not os.path.exists(str(tmpdir / "file"))


def test_safe_edit_copy_with_write_protection(tmpdir):
    os.chdir(str(tmpdir))
    open("asdf", "wb").write(b"csdf")
    with SafeFile("asdf") as f:
        f.use_write_protection()
        f.open_copy("r+b")
        assert f.name != "asdf"
        assert f.read() == b"csdf"
        f.seek(0)
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"

    with pytest.raises(IOError):
        open("asdf", "wb")


def test_safe_edit_inplace_with_write_protection(tmpdir):
    os.chdir(str(tmpdir))
    open("asdf", "wb").write(b"csdf")
    os.chmod("asdf", 0o440)
    with SafeFile("asdf") as f:
        f.use_write_protection()
        f.open_inplace("r+b")
        assert f.name == "asdf"
        assert f.read() == b"csdf"
        f.seek(0)
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"
    with pytest.raises(IOError):
        open("asdf", "wb")


def test_safe_edit_unlinks_copy_on_error(tmpdir):
    os.chdir(str(tmpdir))
    with pytest.raises(ValueError):
        with SafeFile("asdf") as f:
            f.open_new("wb")
            f_name = f.name
            raise ValueError()
    assert not os.path.exists(f_name)


def test_safe_edit_read_write_encoded(tmpdir):
    os.chdir(str(tmpdir))
    open("asdf", "wb").write(b"csdf")
    with SafeFile("asdf", encoding="utf-8") as f:
        f.open_inplace("r+b")
        assert f.read() == "csdf"
        f.write("asdf")

    assert open("asdf", "rb").read() == b"csdfasdf"


def test_safe_edit_truncate(tmpdir):
    os.chdir(str(tmpdir))
    open("asdf", "wb").write(b"csdf")
    with SafeFile("asdf", encoding="utf-8") as f:
        f.open_inplace("r+b")
        assert f.read() == "csdf"
        f.seek(0)
        f.truncate()

    assert open("asdf", "rb").read() == b""


def test_safefile_fileobj_api(tmpdir):
    os.chdir(str(tmpdir))
    with open("asdf", "wb") as seed:
        seed.write(1024 * b"X")
    with SafeFile("asdf", encoding="utf-8") as f:
        f.open_inplace("r+b")
        f.seek(823)
        assert f.tell() == 823
        assert int(f.fileno()) > 2


def test_roughly_compare_files_same(tmpdir):
    os.chdir(str(tmpdir))
    with open("a", "wb") as f:
        f.write(b"asdf" * 100)
    with open("b", "wb") as f:
        f.write(b"asdf" * 100)

    for x in range(20):
        assert files_are_roughly_equal(
            open("a", "rb"), open("b", "rb"), blocksize=10
        )


def test_roughly_compare_files_1_changed_block(tmpdir):
    os.chdir(str(tmpdir))
    with open("a", "wb") as f:
        f.write(b"asdf" * 100)
        f.seek(100)
        f.write(b"bsdf")
    with open("b", "wb") as f:
        f.write(b"asdf" * 100)

    detected = 0
    for x in range(20):
        detected += files_are_roughly_equal(
            open("a", "rb"), open("b", "rb"), blocksize=10
        )

    assert detected > 0 and detected <= 20


def test_roughly_compare_files_timeout(tmpdir):
    os.chdir(str(tmpdir))
    with open("a", "wb") as f:
        f.write(b"asdf" * 100)
    with open("b", "wb") as f:
        f.write(b"bsdf" * 100)

    # The files are different but we don't notice as we run into a timeout.
    # That's fine.
    assert files_are_roughly_equal(open("a", "rb"), open("b", "rb"), timeout=0)
    # Without the timeout we do notice
    assert not files_are_roughly_equal(open("a", "rb"), open("b", "rb"))


def test_copy_overwrite_correctly_makes_sparse_file(tmpdir):
    # Create a test file that contains random data, then we insert
    # blocks of zeroes. copy_overwrite will not break them and will make the
    # file sparse.
    source_name = str(tmpdir / "input")
    with open(source_name, "wb") as f:
        f.write(b"12345" * 1024 * 100)
        f.seek(1024 * 16)
        f.write(b"\x00" * 1024 * 16)
    with open(source_name, "rb") as source:
        target_name = str(tmpdir / "output")
        with open(target_name, "wb") as target:
            # To actually ensure that we punch holes and truncate, lets
            # fill the file with a predictable pattern that is non-zero and
            # longer than the source.
            target.write(b"1" * 1024 * 150)
        with open(target_name, "r+b") as target:
            copy_overwrite(source, target)
    with open(source_name, "rb") as source_current:
        with open(target_name, "rb") as target_current:
            assert source_current.read() == target_current.read()
    if sys.platform == "linux2":
        assert os.stat(source_name).st_blocks > os.stat(target_name).st_blocks
    else:
        assert os.stat(source_name).st_blocks >= os.stat(target_name).st_blocks


def test_unmocked_now_returns_time_time_float():
    before = datetime.datetime.now(ZoneInfo("UTC"))
    now = backy.utils.now()
    after = datetime.datetime.now(ZoneInfo("UTC"))
    assert before <= now <= after
