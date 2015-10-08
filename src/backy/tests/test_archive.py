from backy.archive import Archive
import pytest


@pytest.fixture
def archive(tmpdir):
    return Archive(str(tmpdir))


def test_empty_revisions(archive):
    archive.scan()
    assert archive.history == []


def test_find_revision_empty(archive):
    archive.scan()
    with pytest.raises(KeyError):
        archive.find_revision(-1)
    with pytest.raises(KeyError):
        archive.find_revision("last")
    with pytest.raises(KeyError):
        archive.find_revision("fdasfdka")


def test_load_revisions(archive, tmpdir):
    with open(str(tmpdir / '123-123.rev'), 'wb') as f:
        f.write(b"""\
uuid: 123-123
timestamp: 2015-08-29 00:00:00
parent:
""")
    with open(str(tmpdir / '124-124.rev'), 'wb') as f:
        f.write(b"""\
uuid: 124-124
timestamp: 2015-08-30 00:00:00
parent: 123-123
""")

    archive.scan()
    assert [x.uuid for x in archive.history] == ["123-123", "124-124"]

    assert archive.history[1].uuid == "124-124"
    assert archive.history[1].parent == "123-123"

    assert archive.find_revisions("all") == archive.history

    assert archive.find_revision("last").uuid == "124-124"
    assert archive.find_revision(0).uuid == "124-124"
    assert archive.find_revision(1).uuid == "123-123"
    assert archive.find_revision("124-124").uuid == "124-124"

    with pytest.raises(KeyError):
        archive.find_revision("125-125")

    assert archive.find_revisions(1) == [
        archive.find_revision(1)]


def test_clean_history_should_exclude_incomplete_revs(archive, tmpdir):
    with open(str(tmpdir / '123-123.rev'), 'wb') as f:
        f.write(b"""\
parent: 456-456
stats: {bytes_written: 0}
tags: [daily]
timestamp: 2015-10-07 14:04:50.094479+00:00
uuid: 123-123
""")
    with open(str(tmpdir / '456-456.rev'), 'wb') as f:
        f.write(b"""\
parent: null
stats: {bytes_written: 524280, duration: 0.014884233474731445}
tags: [test]
timestamp: 2015-10-07 12:54:12.566326+00:00
uuid: 456-456
""")
    archive.scan()
    assert 1 == len(archive.clean_history)
    assert '456-456' == archive.clean_history[0].uuid
