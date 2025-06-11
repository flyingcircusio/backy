import asyncio
import datetime
import os
from zoneinfo import ZoneInfo

import pytest

import backy.utils
from backy.tests import Ellipsis
from backy.utils import (
    AdjustableBoundedSemaphore,
    SafeFile,
    TimeOut,
    TimeOutError,
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
    for x in range(200):
        detected += not files_are_roughly_equal(
            open("a", "rb"), open("b", "rb"), blocksize=10
        )

    assert 0 < detected <= 200


def test_roughly_compare_files_size_mismatch(tmp_path):
    os.chdir(str(tmp_path))
    with open("a", "wb") as f:
        f.write(b"asdf" * 100)
        f.write(b"bsdf")
    with open("b", "wb") as f:
        f.write(b"asdf" * 100)

    assert not files_are_roughly_equal(
        open("a", "rb"), open("b", "rb"), blocksize=10
    )


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


def test_unmocked_now_returns_time_time_float():
    before = datetime.datetime.now(ZoneInfo("UTC"))
    now = backy.utils.now()
    after = datetime.datetime.now(ZoneInfo("UTC"))
    assert before <= now <= after


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


async def test_adjustable_bound_semaphore_simple():
    async def acquire(sem, num, assert_full=True):
        for _ in range(num):
            assert not sem.locked()
            await sem.acquire()
        if assert_full:
            assert sem.locked()

    def release(sem, num, assert_empty=True):
        for _ in range(num):
            sem.release()
        if assert_empty:
            with pytest.raises(ValueError):
                sem.release()

    sem = AdjustableBoundedSemaphore(3)
    sem.adjust(5)
    sem.adjust(2)
    release(sem, 0)
    await acquire(sem, 2)
    release(sem, 2)

    sem.adjust(4)
    await acquire(sem, 2, assert_full=False)
    # bound: 4, value: 2
    sem.adjust(0)
    t = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0)
    assert not t.done()
    assert sem.locked()

    release(sem, 2)
    await asyncio.sleep(0)
    assert not t.done()
    assert sem.locked()

    sem.adjust(1)
    await t
    assert sem.locked()


async def test_adjustable_bound_semaphore_wait():
    sem = AdjustableBoundedSemaphore(1)

    await sem.acquire()
    t = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0)
    assert not t.done()

    sem.release()
    # task t gets immediately scheduled
    assert sem._value == 0

    sem.adjust(0)
    # ... and continues to execute
    await t
    assert sem._value == 0


async def test_adjustable_bound_semaphore_cancel():
    """task is scheduled but cancelled before it can acquire"""
    sem = AdjustableBoundedSemaphore(1)

    await sem.acquire()
    t = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0)
    assert not t.done()

    sem.release()
    # task t gets immediately scheduled
    assert sem._value == 0

    t.cancel()
    sem.adjust(0)
    with pytest.raises(asyncio.CancelledError):
        await t
    # this is a bug: releases from scheduled but cancelled tasks will not be intercepted
    assert sem._value == 1

    await sem.acquire()
    sem.release()
    # it will however converge on the next release call
    assert sem._value == 0
