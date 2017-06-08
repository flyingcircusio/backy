from backy.fallocate import punch_hole
from collections import namedtuple
import struct
import logging


log = logging.getLogger(__name__)


def unpack_from(fmt, f):
    size = struct.calcsize(fmt)
    b = f.read(size)
    return struct.unpack(fmt, b)


Zero = namedtuple('Zero', ['start', 'length'])
Data = namedtuple('Data', ['start', 'length', 'stream'])
SnapSize = namedtuple('SnapSize', ['size'])
FromSnap = namedtuple('FromSnap', ['snapshot'])
ToSnap = namedtuple('ToSnap', ['snapshot'])


class RBDDiffV1(object):

    phase = None    # header, metadata, data

    header = b'rbd diff v1\n'

    def __init__(self, fh):
        # self.filename = filename
        self.f = fh

        self.phase = 'header'
        self.read_header()
        self.record_type = None
        self._streaming = False

    def read_header(self):
        assert self.phase == 'header'
        header = self.f.read(len(self.header))
        if header != self.header:
            raise ValueError('Unexpected header: {0!r}'.format(header))
        self.phase = 'metadata'

    def read_record(self):
        assert not self._streaming, "Unread data from read_w. Consume first."
        self.last_record_type = self.record_type
        self.record_type = self.f.read(1).decode('ascii')
        if self.record_type not in ['f', 't', 's', 'w', 'z', 'e']:
            raise ValueError(
                'Got invalid record type "{}". '
                'Previous record: {}'.format(
                    self.record_type, self.last_record_type))
        method = getattr(self, 'read_{}'.format(self.record_type))
        return method()

    def read_fbytes(self, encoding=None):
        length = unpack_from('<i', self.f)[0]
        data = self.f.read(length)
        if encoding is not None:
            data = data.decode(encoding)
        return data

    def read_f(self):
        "from snap"
        assert self.phase == 'metadata'
        return FromSnap(self.read_fbytes('ascii'))

    def read_t(self):
        "to snap"
        assert self.phase == 'metadata'
        return ToSnap(self.read_fbytes('ascii'))

    def read_s(self):
        "size"
        assert self.phase == 'metadata'
        return SnapSize(unpack_from('<Q', self.f)[0])

    def read_e(self):
        self.phase = 'end'
        raise StopIteration()

    def read_w(self):
        "updated data"
        if self.phase == 'metadata':
            self.phase = 'data'
        offset, length = unpack_from('<QQ', self.f)

        def stream():
            remaining = length
            while remaining:
                read = min(4 * 1024 ** 2, remaining)
                chunk = self.f.read(read)
                remaining = remaining - read
                yield chunk
            self._streaming = False

        self._streaming = True
        return Data(offset, length, stream)

    def read_z(self):
        "zero data"
        if self.phase == 'metadata':
            self.phase = 'data'
        offset, length = unpack_from('<QQ', self.f)
        return Zero(offset, length)

    def read_metadata(self):
        while True:
            record = self.read_record()
            if self.phase != 'metadata':
                self.first_data_record = record
                return
            yield record

    def read_data(self):
        if self.phase == 'end':
            raise StopIteration()
        yield self.first_data_record
        while True:
            yield self.read_record()

    def integrate(self, target, snapshot_from, snapshot_to, clean=True):
        """Integrate this diff into the given target.

        If clean is set (default: True) then remove the delta after a
        successful integration.

        """
        bytes = 0

        for record in self.read_metadata():
            if isinstance(record, SnapSize):
                target.seek(record.size)
                target.truncate()
            elif isinstance(record, FromSnap):
                assert record.snapshot == snapshot_from
            elif isinstance(record, ToSnap):
                assert record.snapshot == snapshot_to

        for record in self.read_data():
            target.seek(record.start)
            if isinstance(record, Zero):
                punch_hole(target, target.tell(), record.length)
            elif isinstance(record, Data):
                for chunk in record.stream():
                    target.write(chunk)
            bytes += record.length

        self.f.close()
        return bytes
