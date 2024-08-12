import datetime
import os
import sys
from zoneinfo import ZoneInfo

import pytest

import backy.utils
from backy.tests import Ellipsis
from backy.utils import (
    SafeFile,
    TimeOut,
    TimeOutError,
    _fake_fallocate,
    copy_overwrite,
    files_are_equal,
    files_are_roughly_equal,
    punch_hole,
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


def test_compare_files_same(tmp_path):
    os.chdir(str(tmp_path))
    with open("a", "wb") as f:
        f.write(b"asdf")
    with open("b", "wb") as f:
        f.write(b"asdf")

    assert files_are_equal(open("a", "rb"), open("b", "rb"))


def test_compare_files_different_content(tmp_path):
    os.chdir(str(tmp_path))
    with open("a", "wb") as f:
        f.write(b"asdf")
    with open("b", "wb") as f:
        f.write(b"bsdf")

    assert not files_are_equal(open("a", "rb"), open("b", "rb"))


def test_compare_files_different_length(tmp_path):
    os.chdir(str(tmp_path))
    with open("a", "wb") as f:
        f.write(b"asdf1")
    with open("b", "wb") as f:
        f.write(b"bsdf")

    assert not files_are_equal(open("a", "rb"), open("b", "rb"))


def test_safe_writable_rename_no_writeprotect(tmp_path):
    os.chdir(str(tmp_path))
    with SafeFile("asdf") as f:
        f.open_new("wb")
        assert f.name != "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"


def test_safe_writable_no_rename_no_writeprotect(tmp_path):
    os.chdir(str(tmp_path))
    with SafeFile("asdf") as f:
        f.open_inplace("wb")
        assert f.name == "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"


def test_safe_writable_no_rename_no_writeprotect_existing_file(tmp_path):
    os.chdir(str(tmp_path))
    open("asdf", "wb").write(b"bsdf")
    with SafeFile("asdf") as f:
        f.open_inplace("r+b")
        assert f.read() == b"bsdf"
        f.seek(0)
        assert f.name == "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"


def test_safe_writable_rename_writeprotect(tmp_path):
    os.chdir(str(tmp_path))
    with SafeFile("asdf") as f:
        f.use_write_protection()
        f.open_new("wb")
        assert f.name != "asdf"
        f.write(b"asdf")

    assert open("asdf", "rb").read() == b"asdf"

    with pytest.raises(IOError):
        open("asdf", "wb")


def test_safe_edit_noop(tmp_path):
    with SafeFile(str(tmp_path / "file")):
        pass
    assert not os.path.exists(str(tmp_path / "file"))


def test_safe_edit_copy_with_write_protection(tmp_path):
    os.chdir(str(tmp_path))
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


def test_safe_edit_inplace_with_write_protection(tmp_path):
    os.chdir(str(tmp_path))
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


def test_safe_edit_unlinks_copy_on_error(tmp_path):
    os.chdir(str(tmp_path))
    with pytest.raises(ValueError):
        with SafeFile("asdf") as f:
            f.open_new("wb")
            f_name = f.name
            raise ValueError()
    assert not os.path.exists(f_name)


def test_safe_edit_read_write_encoded(tmp_path):
    os.chdir(str(tmp_path))
    open("asdf", "wb").write(b"csdf")
    with SafeFile("asdf", encoding="utf-8") as f:
        f.open_inplace("r+b")
        assert f.read() == "csdf"
        f.write("asdf")

    assert open("asdf", "rb").read() == b"csdfasdf"


def test_safe_edit_truncate(tmp_path):
    os.chdir(str(tmp_path))
    open("asdf", "wb").write(b"csdf")
    with SafeFile("asdf", encoding="utf-8") as f:
        f.open_inplace("r+b")
        assert f.read() == "csdf"
        f.seek(0)
        f.truncate()

    assert open("asdf", "rb").read() == b""


def test_safefile_fileobj_api(tmp_path):
    os.chdir(str(tmp_path))
    with open("asdf", "wb") as seed:
        seed.write(1024 * b"X")
    with SafeFile("asdf", encoding="utf-8") as f:
        f.open_inplace("r+b")
        f.seek(823)
        assert f.tell() == 823
        assert int(f.fileno()) > 2


def test_roughly_compare_files_same(tmp_path):
    os.chdir(str(tmp_path))
    with open("a", "wb") as f:
        f.write(b"asdf" * 100)
    with open("b", "wb") as f:
        f.write(b"asdf" * 100)

    for x in range(20):
        assert files_are_roughly_equal(
            open("a", "rb"), open("b", "rb"), blocksize=10
        )


def test_roughly_compare_files_1_changed_block(tmp_path):
    os.chdir(str(tmp_path))
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


def test_roughly_compare_files_timeout(tmp_path):
    os.chdir(str(tmp_path))
    with open("a", "wb") as f:
        f.write(b"asdf" * 100)
    with open("b", "wb") as f:
        f.write(b"bsdf" * 100)

    # The files are different but we don't notice as we run into a timeout.
    # That's fine.
    assert files_are_roughly_equal(open("a", "rb"), open("b", "rb"), timeout=0)
    # Without the timeout we do notice
    assert not files_are_roughly_equal(open("a", "rb"), open("b", "rb"))


def test_copy_overwrite_correctly_makes_sparse_file(tmp_path):
    # Create a test file that contains random data, then we insert
    # blocks of zeroes. copy_overwrite will not break them and will make the
    # file sparse.
    source_name = str(tmp_path / "input")
    with open(source_name, "wb") as f:
        f.write(b"12345" * 1024 * 100)
        f.seek(1024 * 16)
        f.write(b"\x00" * 1024 * 16)
    with open(source_name, "rb") as source:
        target_name = str(tmp_path / "output")
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


@pytest.fixture
def testfile(tmp_path):
    fn = str(tmp_path / "myfile")
    with open(fn, "wb") as f:
        f.write(b"\xde\xad\xbe\xef" * 32)
    return fn


def test_punch_hole(testfile):
    with open(testfile, "r+b") as f:
        f.seek(0)
        punch_hole(f, 2, 4)
        f.seek(0)
        assert f.read(8) == b"\xde\xad\x00\x00\x00\x00\xbe\xef"


def test_punch_hole_needs_length(testfile):
    with pytest.raises(IOError):
        with open(testfile, "r+b") as f:
            punch_hole(f, 10, 0)


def test_punch_hole_needs_writable_file(testfile):
    with pytest.raises(OSError):
        with open(testfile, "rb") as f:
            punch_hole(f, 0, 1)


def test_punch_hole_needs_nonnegative_offset(testfile):
    with pytest.raises(OSError):
        with open(testfile, "r+b") as f:
            punch_hole(f, -1, 1)


def test_fake_fallocate_only_punches_holes(testfile):
    with pytest.raises(NotImplementedError):
        with open(testfile, "r+b") as f:
            _fake_fallocate(f, 0, 0, 10)


def test_timeout(capsys):
    timeout = TimeOut(0.05, 0.01)
    while timeout.tick():
        print("tick")
    assert timeout.timed_out
    out, err = capsys.readouterr()
    assert "tick\ntick\ntick" in out


def test_raise_on_timeout(capsys):
    timeout = TimeOut(0.05, 0.01, raise_on_timeout=True)
    with pytest.raises(TimeOutError):
        while True:
            timeout.tick()
            print("tick")
    out, err = capsys.readouterr()
    assert "tick\ntick\ntick" in out
