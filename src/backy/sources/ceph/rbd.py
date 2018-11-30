from ...ext_deps import RBD
from ...utils import CHUNK_SIZE
from .diff import RBDDiffV1
from packaging.version import Version
import backy.sources.ceph
import contextlib
import json
import logging
import subprocess

logger = logging.getLogger(__name__)


class RBDClient(object):

    def _rbd(self, cmd, format=None):
        cmd = filter(None, cmd)
        rbd = [RBD]

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

    @contextlib.contextmanager
    def export_diff(self, new, old):
        logger.info(str(backy.sources.ceph.CEPH_VERSION))
        if backy.sources.ceph.CEPH_VERSION >= Version("10.0"):
            # Introduced in Jewel, major version 10
            EXPORT_WHOLE_OBJECT = ['--whole-object']
        else:
            EXPORT_WHOLE_OBJECT = []
        proc = subprocess.Popen(
            [RBD,
             'export-diff', new,
             '--from-snap', old] + EXPORT_WHOLE_OBJECT + ['-'],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            # Have a rather largish buffer size, so rbd has some room to
            # push its data to, when we are busy writing.
            bufsize=8*CHUNK_SIZE)
        try:
            yield RBDDiffV1(proc.stdout)
        finally:
            proc.stdout.close()
            proc.wait()

    @contextlib.contextmanager
    def image_reader(self, image):
        mapped = self.map(image, readonly=True)
        source = open(mapped['device'], 'rb', buffering=CHUNK_SIZE)
        try:
            yield source
        finally:
            source.close()
            self.unmap(mapped['device'])

    @contextlib.contextmanager
    def export(self, image):
        proc = subprocess.Popen(
            [RBD,
             'export', image,
             '-'],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            # Have a rather largish buffer size, so rbd has some room to
            # push its data to, when we are busy writing.
            bufsize=4*CHUNK_SIZE)
        try:
            yield proc.stdout
        finally:
            proc.stdout.close()
            proc.wait()
