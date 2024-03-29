import shutil

import pytest


@pytest.fixture
def backup_with_revisions(backup, tmpdir):
    with open(str(tmpdir / "123-0.rev"), "wb") as f:
        f.write(
            b"""\
uuid: 123-0
timestamp: 2015-08-29 00:00:00+00:00
parent:
trust: verified
stats: {bytes_written: 14868480, duration: 31.1}
tags: [daily, weekly, monthly]
"""
        )
    with open(str(tmpdir / "123-1.rev"), "wb") as f:
        f.write(
            b"""\
uuid: 123-1
timestamp: 2015-08-30 01:00:00+00:00
parent: 123-0
stats: {bytes_written: 1486880, duration: 3.7}
tags: [daily, weekly]
"""
        )
    with open(str(tmpdir / "123-2.rev"), "wb") as f:
        f.write(
            b"""\
uuid: 123-2
timestamp: 2015-08-30 02:00:00+00:00
parent: 123-1
stats: {}
tags: [daily]
"""
        )
    backup.scan()
    return backup


def test_empty_revisions(backup):
    assert backup.history == []


def test_find_revision_empty(backup):
    with pytest.raises(KeyError):
        backup.find("-1")
    with pytest.raises(KeyError):
        backup.find("last")
    with pytest.raises(KeyError):
        backup.find("fdasfdka")


def test_load_revisions(backup_with_revisions):
    a = backup_with_revisions
    assert [x.uuid for x in a.history] == ["123-0", "123-1", "123-2"]
    assert a.history[1].uuid == "123-1"
    assert a.history[1].get_parent().uuid == "123-0"
    assert a.history[2].get_parent().uuid == "123-1"
    assert a.history[0].get_parent() is None


def test_find_revisions(backup_with_revisions):
    a = backup_with_revisions
    assert a.find_revisions("all") == a.history
    assert a.find_revisions("1") == [a.find("1")]
    assert a.find_revisions("tag:dail") == []
    assert a.find_revisions("trust:verified") == [a.find("123-0")]
    assert a.find_revisions("2..1") == [a.find("2"), a.find("1")]
    assert a.find_revisions("1..2") == [a.find("2"), a.find("1")]
    assert a.find_revisions("123-0..123-1") == [
        a.find("123-0"),
        a.find("123-1"),
    ]
    assert a.find_revisions("last(tag:daily)..123-1") == [
        a.find("123-1"),
        a.find("123-2"),
    ]
    assert a.find_revisions("123-1..") == [a.find("123-1"), a.find("123-2")]
    assert a.find_revisions("..") == a.history
    assert a.find_revisions("first..last") == a.history
    assert a.find_revisions("tag:weekly") == [a.find("123-0"), a.find("123-1")]
    assert a.find_revisions("1, tag:weekly") == [
        a.find("123-1"),
        a.find("123-0"),
    ]
    assert a.find_revisions("0,2..1") == [
        a.find("123-2"),
        a.find("123-0"),
        a.find("123-1"),
    ]
    assert a.find_revisions("2,1, 2,0,1") == [
        a.find("123-0"),
        a.find("123-1"),
        a.find("123-2"),
    ]
    assert a.find_revisions("2015-09-01..2015-08-30") == [
        a.find("123-1"),
        a.find("123-2"),
    ]
    assert a.find_revisions("2015-08-30..last(last(tag:daily&clean))") == [
        a.find("123-1"),
    ]
    assert a.find_revisions("2015-08-30..,trust:verified") == [
        a.find("123-1"),
        a.find("123-2"),
        a.find("123-0"),
    ]
    assert a.find_revisions(
        "first(trust:verified)..last(reverse(2015-08-30..))"
    ) == [
        a.find("123-0"),
        a.find("123-1"),
    ]
    assert a.find_revisions("reverse(not(clean))") == [
        a.find("123-2"),
    ]
    assert a.find_revisions("last(reverse(first(123-1, 123-0)))") == [
        a.find("123-1"),
    ]
    assert a.find_revisions("( (first( (123-0, 123-1)) ))") == [
        a.find("123-0"),
    ]


def test_find_revisions_should_raise_invalid_spec(backup_with_revisions):
    a = backup_with_revisions
    with pytest.raises(KeyError):
        a.find_revisions("aaaa..125")
    with pytest.raises(AssertionError):
        a.find_revisions("last)..5")
    with pytest.raises(KeyError):
        a.find_revisions("clean-..,1")
    with pytest.raises(KeyError):
        a.find_revisions("123-")
    with pytest.raises(IndexError):
        a.find_revisions("first(not(all))")
    with pytest.raises(KeyError):
        a.find_revisions("2015-09..2015-08-30")


def test_find_revision(backup_with_revisions):
    a = backup_with_revisions
    assert a.find("last").uuid == "123-2"
    with pytest.raises(KeyError):
        a.find("-1")
    assert a.find("0").uuid == "123-2"
    assert a.find("1").uuid == "123-1"
    assert a.find("2").uuid == "123-0"

    assert a.find("123-1").uuid == "123-1"
    with pytest.raises(KeyError):
        a.find("125-125")

    assert a.find("last(tag:daily)").uuid == "123-2"
    assert a.find("last(tag:weekly)").uuid == "123-1"
    assert a.find("last(tag:monthly)").uuid == "123-0"
    assert a.find(" first( tag:monthly  ) ").uuid == "123-0"


def test_clean_history_should_exclude_incomplete_revs(backup_with_revisions):
    assert 2 == len(backup_with_revisions.clean_history)


def test_ignore_duplicates(backup_with_revisions, tmpdir):
    shutil.copy(str(tmpdir / "123-2.rev"), str(tmpdir / "123-3.rev"))
    a = backup_with_revisions
    a.scan()
    assert 3 == len(a.history)
