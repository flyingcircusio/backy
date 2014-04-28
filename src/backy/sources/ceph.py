from subprocess import check_output
import json
import os
import struct
import logging
import sys


logger = logging.getLogger(__name__)


cmd = check_output


def unpack_from(fmt, f):
    size = struct.calcsize(fmt)
    b = f.read(size)
    return struct.unpack(fmt, b)


class RBDDiffV1N(object):

    phase = None    # header, metadata, data

    header = b'rbd diff v1\n'

    def __init__(self, filename):
        self.filename = filename
        self.f = open(self.filename, 'rb')

        self.phase = 'header'
        self.read_header()

    def read_header(self):
        assert self.phase == 'header'
        header = self.f.read(len(self.header))
        if header != self.header:
            raise ValueError('Unexpected header: {0!r}'.format(header))
        self.phase = 'metadata'

    def read_record(self):
        assert self.phase in ['metadata', 'data']
        record_type = self.f.read(1)
        if record_type not in [b'f', b't', b's', b'w', b'z', b'e']:
            raise ValueError('Got record type {0!r}'.format(record_type))
        if record_type == b'e':
            raise StopIteration
        return getattr(self, 'read_{}'.format(record_type.decode('ascii')))()

    def read_fbytes(self, encoding=None):
        length = unpack_from('<i', self.f)[0]
        data = self.f.read(length)
        if encoding is not None:
            data = data.decode(encoding)
        return data

    def read_f(self):
        "from snap"
        assert self.phase == 'metadata'
        return ('from snap', self.read_fbytes('ascii'))

    def read_t(self):
        "to snap"
        assert self.phase == 'metadata'
        return ('to snap', self.read_fbytes('ascii'))

    def read_s(self):
        "size"
        assert self.phase == 'metadata'
        return ('snap size', unpack_from('<Q', self.f)[0])

    def read_w(self):
        "updated data"
        if self.phase == 'metadata':
            self.phase = 'data'
        # XXX careful with reading all of that at once ...
        offset, length = unpack_from('<QQ', self.f)
        return ('data', offset, length, self.f.read(length))

    def read_z(self):
        "zero data"
        if self.phase == 'metadata':
            self.phase = 'data'
        offset, length = unpack_from('<QQ', self.f)
        return ('zero', offset, length)

    def __iter__(self):
        return self

    def __next__(self):
        return self.read_record()


class CephRBD(object):

    def __init__(self, config):
        self.pool = config['pool']
        self.image = config['image']

    @property
    def _image_name(self):
        return '{}/{}'.format(self.pool, self.image)

    def _snapshot_image_name(self, uuid=None):
        if uuid is None:
            uuid = self.revision.uuid
        return '{}@backy-{}'.format(self._image_name, uuid)

    def _rbd_cmd(self, command, format=None):
        command = command.format(
            image_name=self._image_name,
            snapshot=self._snapshot_image_name())
        if format == 'json':
            command = ' --format json ' + command
        command = 'rbd --no-progress ' + command
        result = cmd(command, shell=True)
        if format == 'json':
            result = json.loads(result.decode('utf-8'))
        return result

    def _rbd_ls_snap(self):
        return self._rbd_cmd('snap ls {image_name}', format='json')

    def backup(self, revision):
        self.revision = revision

        self.create_snapshot()
        self.diff()
        self.clean_snapshots()

    def create_snapshot(self):
        try:
            self._rbd_find_snapshot(self.revision.uuid)
            logger.info('Re-using existing snapshot {}'.format(
                        self.revision.uuid))
        except KeyError:
            logger.info('Creating snapshot {}'.format(self.revision.uuid))
            self._rbd_cmd('snap create {}'.format(self._snapshot_image_name()))

    def clean_snapshots(self):
        # Remove all snapshots but the one we just used to backup.
        for snapshot in self._rbd_ls_snap():
            if not snapshot['name'].startswith('backy-'):
                continue
            uuid = snapshot['name'].replace('backy-', '')
            if uuid != self.revision.uuid:
                logger.info('Removing snapshot {}'.format(uuid))
                self._rbd_cmd('snap rm {}'.format(
                    self._snapshot_image_name(uuid)))

    def diff(self):
        if not self.revision.parent:
            logger.info('No parent revision known. Reading full snapshot.')
            self.full()
            return

        try:
            self._rbd_find_snapshot(self.revision.parent)
        except KeyError:
            logger.info(
                'Parent snapshot does not exist. Reading full snapshot.')
            self.full()
            return

        logger.info('Exporting Ceph snapshot diff')
        self._rbd_cmd('export-diff {new} {output} --from-snap {old}'.format(
            new=self._snapshot_image_name(),
            old='backy-{}'.format(self.revision.parent),
            output=self.revision.filename+'.rbddiff'))

        logger.info('Integrating snapshot diff into base image')
        diff = RBDDiffV1N(self.revision.filename+'.rbddiff')
        diff = iter(diff)

        target = open(self.revision.filename, 'r+b')

        # Read metadata
        for record in diff:
            if diff.phase != 'metadata':
                break

            if record[0] == 'snap size':
                target.seek(record[1])
                target.truncate()
            elif record[0] == 'from snap':
                assert record[1] == 'backy-'+self.revision.parent
            elif record[0] == 'to snap':
                assert record[1] == 'backy-'+self.revision.uuid
            else:
                raise ValueError('Unexpected record {0!r}'.format(record))

        self.revision.stats['bytes_written'] = 0

        # Read data
        for data in diff:
            if data[0] == 'zero':
                target.seek(data[1])
                # XXX fadvice hole punching
                target.write('\0'*data[2])
                self.revision.stats['bytes_written'] += data[2]

            elif data[0] == 'data':
                target.seek(data[1])
                # XXX make constant consumption
                target.write(data[3])
                self.revision.stats['bytes_written'] += data[2]

        os.unlink(self.revision.filename+'.rbddiff')

        target.flush()
        os.fsync(target)

    def _rbd_find_mapping(self, **kw):
        mappings = self._rbd_cmd('showmapped', format='json')
        for mapping in mappings.values():
            matches = True
            for key, value in kw.items():
                if mapping.get(key, None) != value:
                    matches = False
            if matches:
                return mapping
        raise KeyError('No mapping found with {}'.format(kw))

    def _rbd_find_snapshot(self, uuid):
        snapshots = self._rbd_ls_snap()
        for snapshot in snapshots:
            if not snapshot['name'].startswith('backy-'):
                continue
            snapshot_uuid = snapshot['name'].replace('backy-', '')
            if snapshot_uuid == uuid:
                return snapshot
        raise KeyError(uuid)

    def full(self):
        self._rbd_cmd('map --read-only {snapshot}')

        mapped = self._rbd_find_mapping(
            pool=self.pool, name=self.image, snap='backy-'+self.revision.uuid)

        self.revision.stats['bytes_written'] = 0

        source = open(mapped['device'], 'rb')
        # We are guaranteed that _a_ file exists at this point - CoW semantics
        # like us to avoid accidentally creating a new file.
        target = open(self.revision.filename, 'r+b')
        while True:
            chunk = source.read(4*1024**2)
            self.revision.stats['bytes_written'] += len(chunk)
            print('.', end='')
            sys.stdout.flush()
            if not chunk:
                break
            target.write(chunk)

        target.flush()
        os.fsync(target)
        source.close()

        self._rbd_cmd('unmap {}'.format(mapped['device']))
