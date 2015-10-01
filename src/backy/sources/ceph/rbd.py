from ...ext_deps import RBD
from .diff import RBDDiffV1
import contextlib
import json
import subprocess


import logging

logger = logging.getLogger(__name__)


class RBDClient(object):

    def _rbd(self, cmd, format=None):
        cmd = filter(None, cmd)
        rbd = [RBD, '--no-progress']

        if format == 'json':
            rbd.append('--format=json')

        rbd.extend(cmd)

        logger.debug(' '.join(rbd))
        result = subprocess.check_output(rbd)

        if format == 'json':
            result = json.loads(result.decode('utf-8'))

        return result

    def exists(self, image):
        try:
            return self._rbd(['info', image], format='json')
        except subprocess.CalledProcessError as e:
            if e.returncode == 2:
                return False
            raise

    def map(self, image, readonly=False):
        self._rbd(['--read-only' if readonly else '', 'map', image])

        mappings = self._rbd(['showmapped'], format='json')
        for mapping in mappings.values():
            if image == '{pool}/{name}@{snap}'.format(**mapping):
                return mapping
        raise RuntimeError('Map not found in mapping list.')

    def unmap(self, device):
        self._rbd(['unmap', device])

    def snap_create(self, image):
        self._rbd(['snap', 'create', image])

    def snap_ls(self, image):
        return self._rbd(['snap', 'ls', image], format='json')

    def snap_rm(self, image):
        return self._rbd(['snap', 'rm', image])

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
            if not source.closed:
                source.close()
            self.unmap(mapped['device'])
