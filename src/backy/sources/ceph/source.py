from .rbd import RBDClient
from backy.utils import safe_copy, SafeWritableFile, compare_files
import logging


logger = logging.getLogger(__name__)


class CephRBD(object):

    def __init__(self, config):
        self.pool = config['pool']
        self.image = config['image']
        self.rbd = RBDClient()

    @staticmethod
    def config_from_cli(spec):
        pool, image = spec.split('/')
        return dict(pool=pool, image=image)

    def __call__(self, revision):
        self.revision = revision
        return self

    def __enter__(self):
        self.rbd.snap_create(self._image_name+'@backy-'+self.revision.uuid)
        return self

    @property
    def _image_name(self):
        return '{}/{}'.format(self.pool, self.image)

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        for snapshot in self.rbd.snap_ls(self._image_name):
            if not snapshot['name'].startswith('backy-'):
                continue
            uuid = snapshot['name'].replace('backy-', '')
            if uuid != self.revision.uuid:
                self.rbd.snap_rm(self._image_name+'@'+snapshot['name'])

    def backup(self):
        try:
            parent = self.revision.backup.find_revision(self.revision.parent)
            if not self.rbd.exists(self._image_name+'@backy-'+parent.uuid):
                raise KeyError()
        except KeyError:
                self.full()
        else:
            self.diff()

    def diff(self):
        logger.info('Performing differential backup ...')
        d = self.rbd.export_diff()
        t = SafeWritableFile(self.revision.filename, rename=False, mode='r+b')
        with d as diff, t as target:
            bytes = diff.integrate(target)
        self.revision.stats['bytes_written'] = bytes

    def full(self):
        logger.info('Performing full backup')
        s = self.rbd.image_reader('{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid))
        t = open(self.revision.filename, 'r+b')
        with s as source, t as target:
            bytes = safe_copy(source, target)
        self.revision.stats['bytes_written'] = bytes

    def verify(self):
        return
        logger.info('Performing full verification ...')
        s = self.rbd.image_reader('{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid))
        t = open(self.revision.filename, 'rb')
        with s as source, t as target:
            return compare_files(source, target)
