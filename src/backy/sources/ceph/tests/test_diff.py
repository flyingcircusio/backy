from backy.sources.ceph.diff import Data, Zero
from backy.sources.ceph.diff import FromSnap, ToSnap, SnapSize
from backy.sources.ceph.diff import unpack_from, RBDDiffV1
import io
import os
import os.path
import pytest
import struct


def test_unpack_from_i():
    b = io.BytesIO()
    b.write(struct.pack('<i', 12))
    b.seek(0)
    assert unpack_from('<i', b) == (12,)


def test_unpack_from_qq():
    b = io.BytesIO()
    b.write(struct.pack('<QQ', 12**10, 12**9))
    b.seek(0)
    assert unpack_from('<QQ', b) == (12**10, 12**9)


def test_unpack_from_q():
    b = io.BytesIO()
    b.write(struct.pack('<Q', 12**10))
    b.seek(0)
    assert unpack_from('<Q', b) == (12**10,)


def test_read_header_valid(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd diff v1\n')

    # Simply ensure that the header doesn't blow up.
    RBDDiffV1(open(filename, 'rb'))


def test_read_header_invalid(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd duff v1\n')

    # Simply ensure that the header doesn't blow up.
    with pytest.raises(ValueError) as e:
        RBDDiffV1(open(filename, 'rb'))
    assert e.value.args[0] == "Unexpected header: b'rbd duff v1\\n'"


@pytest.fixture
def sample_diff(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd diff v1\n')
        # from snap
        f.write(b'f')
        f.write(struct.pack(b'<i', len(b'fromsnapshot')))
        f.write(b'fromsnapshot')

        # to snap
        f.write(b't')
        f.write(struct.pack(b'<i', len('tosnapshot')))
        f.write(b'tosnapshot')

        # size
        f.write(b's')
        f.write(struct.pack(b'<Q', 500))

        # data
        f.write(b'w')
        f.write(struct.pack(b'<QQ', 10, len(b'asdfbsdfcsdf')))
        f.write(b'asdfbsdfcsdf')

        # zero
        f.write(b'z')
        f.write(struct.pack(b'<QQ', 50, 100))

        # data
        f.write(b'w')
        f.write(struct.pack(b'<QQ', 200, len('blablabla')))
        f.write(b'blablabla')

        # end
        f.write(b'e')
    return filename


def test_read_sample(sample_diff):
    # Simply ensure that the header doesn't blow up.
    diff = RBDDiffV1(open(sample_diff, 'rb'))
    assert list(diff.read_metadata()) == [
        FromSnap('fromsnapshot'), ToSnap('tosnapshot'), SnapSize(500)]
    data = diff.read_data()
    d = next(data)
    assert isinstance(d, Data)
    assert d.start == 10
    assert d.length == 12
    list(d.stream())

    d = next(data)
    assert isinstance(d, Zero)
    assert d.start == 50
    assert d.length == 100

    d = next(data)
    assert isinstance(d, Data)
    assert d.start == 200
    assert d.length == 9
    list(d.stream())


def test_read_sample_data(sample_diff):
    diff = RBDDiffV1(open(sample_diff, 'rb'))

    # Consume metadata records
    list(diff.read_metadata())

    # Data record: stream right away
    data = diff.read_data()
    d = next(data)
    assert isinstance(d, Data)
    assert list(d.stream()) == [b'asdfbsdfcsdf']

    # Zero record
    next(data)

    d = next(data)
    assert isinstance(d, Data)
    assert list(d.stream()) == [b'blablabla']
    with pytest.raises(StopIteration):
        next(data)


def test_integrate_sample_data(sample_diff, tmpdir):
    diff = RBDDiffV1(open(sample_diff, 'rb'))

    target = open(str(tmpdir/'target'), 'wb')
    target.write(b'\1'*600)
    target.seek(0)

    with target:
        bytes = diff.integrate(target, 'fromsnapshot', 'tosnapshot')
    assert bytes == 121

    integrated = open(str(tmpdir/'target'), 'rb').read()
    assert len(integrated) == 500

    assert integrated[0:10] == b'\1'*10
    assert integrated[10:22] == b'asdfbsdfcsdf'
    assert integrated[22:50] == b'\1'*28
    assert integrated[50:150] == b'\0'*100
    assert integrated[150:200] == b'\1'*50
    assert integrated[200:209] == b'blablabla'
    assert integrated[209:500] == b'\1'*291


def test_integrate_stops_on_broken_metadata_record(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd diff v1\n')
        # from snap
        f.write(b'f')
        f.write(struct.pack(b'<i', len(b'fromsnapshot')))
        f.write(b'fromsnapshot')

        # broken record
        f.write(b'0')

    diff = RBDDiffV1(open(filename, 'rb'))

    target = open(str(tmpdir/'target'), 'wb')
    target.write(b'\1'*600)
    target.seek(0)
    with pytest.raises(ValueError) as e:
        with target:
            diff.integrate(target, 'fromsnapshot', 'tosnapshot')

    assert (e.value.args[0] ==
            'Got invalid record type "0". Previous record: f')


def test_integrate_stops_on_broken_data_record(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd diff v1\n')
        # from snap
        f.write(b'f')
        f.write(struct.pack(b'<i', len(b'fromsnapshot')))
        f.write(b'fromsnapshot')

        # zero
        f.write(b'z')
        f.write(struct.pack(b'<QQ', 50, 100))

        # broken record
        f.write(b'0')

    diff = RBDDiffV1(open(filename, 'rb'))

    target = open(str(tmpdir/'target'), 'wb')
    target.write(b'\1'*600)
    target.seek(0)
    with pytest.raises(ValueError) as e:
        with target:
            diff.integrate(target, 'fromsnapshot', 'tosnapshot')

    assert (e.value.args[0] ==
            'Got invalid record type "0". Previous record: z')


def test_read_zero_first_data_record(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd diff v1\n')

        # zero
        f.write(b'z')
        f.write(struct.pack(b'<QQ', 20, 5*1024**3))

        # end
        f.write(b'e')

    # Simply ensure that the header doesn't blow up.
    diff = RBDDiffV1(open(filename, 'rb'))
    x = list(diff.read_metadata())
    x = list(diff.read_data())
    assert x == [Zero(start=20, length=5*1024**3)]


def test_read_detects_wrong_record_type(tmpdir):
    filename = str(tmpdir / 'sample.rbddiff')
    with open(filename, 'wb') as f:
        f.write(b'rbd diff v1\n')
        f.write(b'a')

    # Simply ensure that the header doesn't blow up.
    diff = RBDDiffV1(open(filename, 'rb'))
    with pytest.raises(ValueError) as e:
        diff.read_record()
    assert e.value.args[0] == (
        'Got invalid record type "a". Previous record: None')


def test_read_empty_diff(tmpdir):
    diff = RBDDiffV1(open(os.path.dirname(__file__) + '/nodata.rbddiff', 'rb'))
    target = open(str(tmpdir / 'foo'), 'wb')
    diff.integrate(
        target,
        'backy-ed968696-5ab0-4fe0-af1c-14cadab44661',
        'backy-f0e7292e-4ad8-4f2e-86d6-f40dca2aa802',
        clean=False)
