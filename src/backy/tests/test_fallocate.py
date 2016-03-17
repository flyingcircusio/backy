import backy.fallocate
import pytest


@pytest.fixture
def testfile(tmpdir):
    fn = str(tmpdir / 'myfile')
    with open(fn, 'wb') as f:
        f.write(b'\xde\xad\xbe\xef' * 32)
    return fn


def test_punch_hole(testfile):
    with open(testfile, 'r+b') as f:
        f.seek(0)
        backy.fallocate.punch_hole(f, 2, 4)
        f.seek(0)
        assert f.read(8) == b'\xde\xad\x00\x00\x00\x00\xbe\xef'


def test_punch_hole_needs_length(testfile):
    with pytest.raises(IOError):
        with open(testfile, 'r+b') as f:
            backy.fallocate.punch_hole(f, 10, 0)


def test_punch_hole_needs_writable_file(testfile):
    with pytest.raises(OSError):
        with open(testfile, 'rb') as f:
            backy.fallocate.punch_hole(f, 0, 1)


def test_punch_hole_needs_nonnegative_offset(testfile):
    with pytest.raises(OSError):
        with open(testfile, 'r+b') as f:
            backy.fallocate.punch_hole(f, -1, 1)


def test_fake_fallocate_only_punches_holes(testfile):
    with pytest.raises(NotImplementedError):
        with open(testfile, 'r+b') as f:
            backy.fallocate._fake_fallocate(f, 0, 0, 10)
