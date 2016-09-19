from backy.archive import Archive
import pytest
import shutil


@pytest.fixture
def archive(tmpdir):
    a = Archive(str(tmpdir))
    return a


@pytest.fixture
def archive_with_revisions(archive, tmpdir):
    with open(str(tmpdir / '123-0.rev'), 'wb') as f:
        f.write(b"""\
uuid: 123-0
timestamp: 2015-08-29 00:00:00
parent:
stats: {bytes_written: 14868480, duration: 31.1}
tags: [daily, weekly, monthly]
""")
    with open(str(tmpdir / '123-1.rev'), 'wb') as f:
        f.write(b"""\
uuid: 123-1
timestamp: 2015-08-30 01:00:00
parent: 123-0
stats: {bytes_written: 1486880, duration: 3.7}
tags: [daily, weekly]
""")
    with open(str(tmpdir / '123-2.rev'), 'wb') as f:
        f.write(b"""\
uuid: 123-2
timestamp: 2015-08-30 02:00:00
parent: 123-1
stats: {}
tags: [daily]
""")
    archive.scan()
    return archive


def test_empty_revisions(archive):
    assert archive.history == []


def test_find_revision_empty(archive):
    with pytest.raises(KeyError):
        archive[-1]
    with pytest.raises(KeyError):
        archive['last']
    with pytest.raises(KeyError):
        archive['fdasfdka']


def test_load_revisions(archive_with_revisions):
    a = archive_with_revisions
    assert [x.uuid for x in a.history] == ['123-0', '123-1', '123-2']
    assert a.history[1].uuid == "123-1"
    assert a.history[1].parent == "123-0"
    assert a.find_revisions("all") == a.history
    assert a.find_revisions(1) == [a[1]]


def test_find_revision(archive_with_revisions):
    a = archive_with_revisions
    assert a['last'].uuid == '123-2'
    with pytest.raises(KeyError):
        a[-1]
    assert a[0].uuid == '123-2'
    assert a[1].uuid == '123-1'
    assert a[2].uuid == '123-0'

    assert a['123-1'].uuid == '123-1'
    with pytest.raises(KeyError):
        a['125-125']

    assert a['daily'].uuid == '123-2'
    assert a['weekly'].uuid == '123-1'
    assert a['monthly'].uuid == '123-0'


def test_clean_history_should_exclude_incomplete_revs(archive_with_revisions):
    assert 2 == len(archive_with_revisions.clean_history)


def test_ignore_duplicates(archive_with_revisions, tmpdir):
    shutil.copy(str(tmpdir / '123-2.rev'), str(tmpdir / '123-3.rev'))
    a = archive_with_revisions
    a.scan()
    assert 3 == len(a.history)
