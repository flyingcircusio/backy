from .diff import RBDDiffV1
import contextlib
import json


class RBDClient(object):

    def _rbd(self, cmd, format=None):
        cmd = filter(None, cmd)
        rbd = ['rbd', '--no-progress']

        if format == 'json':
            rbd.append('--format json')

        rbd.extend(cmd)
        result = cmd(' '.join(rbd), shell=True)

        if format == 'json':
            result = json.loads(result.decode('utf-8'))

        return result

    def snap_ls(self):
        return self._rbd_cmd('snap ls {image_name}', format='json')

    def map(self, image, readonly=False):
        self._rbd(['map', '--readonly' if readonly else '', image])
        # XXX return mapping dict. can this be read without using
        # showmapped, directly from the map command?

    def snap_create(self, image):
        # XXX handle existing snapshots
        self._rbd(['snap', 'create', image])

    def export_diff(self, new, old, target):
        self._rbd(['export-diff',
                   new,
                   target,
                   '--from-snap', old])
        return RBDDiffV1(target)

    @contextlib.contextmanager
    def image_reader(self, image):
        mapped = self.map(image, readonly=True)
        source = open(mapped['device'], 'rb')
        try:
            yield source
        finally:
            if not source.closed():
                source.close()
            self.unmap(mapped['device'])
