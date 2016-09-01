from .rbd import RBDClient
from ...utils import copy_overwrite, files_are_roughly_equal
import logging
import time

logger = logging.getLogger(__name__)


class CephRBD(object):
    """The Ceph RBD source.

    Manages snapshots corresponding to revisions and provides a verification
    that tries to balance reliability and performance.
    """

    def __init__(self, config):
        self.pool = config['pool']
        self.image = config['image']
        self.always_full = config.get('full-always', False)
        self.rbd = RBDClient()

    @staticmethod
    def config_from_cli(spec):
        logger.debug('CephRBD.config_from_cli(%s)', spec)
        param = spec.split('/')
        if len(param) != 2:
            raise RuntimeError('ceph source must be initialized with '
                               'POOL/IMAGE')
        pool, image = param
        return dict(pool=pool, image=image)

    def __call__(self, revision):
        self.revision = revision
        return self

    def __enter__(self):
        snapname = 'backy-{}'.format(self.revision.uuid)
        self.create_snapshot(snapname)
        return self

    def create_snapshot(self, snapname):
        """An overridable method to allow different ways of creating the
        snapshot.
        """
        self.rbd.snap_create(self._image_name + '@' + snapname)

    @property
    def _image_name(self):
        return '{}/{}'.format(self.pool, self.image)

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        # Clean up all snapshots except the one for the most recent valid
        # revision.
        # Previously we used to remove all snapshots but the one for this
        # revision - which is wrong: broken new revisions would always cause
        # full backups instead of new deltas based on the most recent valid
        # one.
        if not self.always_full and self.revision.archive.history:
            keep_snapshot_revision = self.revision.archive.history[-1]
            keep_snapshot_revision = keep_snapshot_revision.uuid
        else:
            keep_snapshot_revision = None
        for snapshot in self.rbd.snap_ls(self._image_name):
            if not snapshot['name'].startswith('backy-'):
                # Do not touch non-backy snapshots
                continue
            uuid = snapshot['name'].replace('backy-', '')
            if uuid != keep_snapshot_revision:
                time.sleep(3)  # avoid race condition while unmapping
                logger.info('Removing old snapshot %s', snapshot['name'])
                self.rbd.snap_rm(self._image_name + '@' + snapshot['name'])

    def backup(self, target):
        if self.always_full:
            logger.debug('Running always full backups')
            self.full(target)
            return
        try:
            parent = self.revision.archive[self.revision.parent]
            if not self.rbd.exists(self._image_name + '@backy-' + parent.uuid):
                raise KeyError()
        except KeyError:
            logger.info('Could not find snapshot for previous revision.')
            self.full(target)
            return
        self.diff(target)

    def diff(self, target):
        logger.info('Performing differential backup')
        snap_from = 'backy-' + self.revision.parent
        snap_to = 'backy-' + self.revision.uuid
        d = self.rbd.export_diff(self._image_name + '@' + snap_to,
                                 snap_from,
                                 self.revision.filename + '.rbddiff')
        t = target.open('r+b')
        with t as target:
            bytes = d.integrate(target, snap_from, snap_to)

        self.revision.stats['bytes_written'] = bytes

    def full(self, target):
        logger.info('Performing full backup')
        s = self.rbd.image_reader('{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid))
        t = target.open('r+b')
        with s as source, t as target:
            bytes = copy_overwrite(source, target)
        self.revision.stats['bytes_written'] = bytes

    def verify(self, target):
        s = self.rbd.image_reader('{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid))
        t = target.open('rb')

        self.revision.stats['ceph-verification'] = 'partial'

        with s as source, t as target:
            logger.info('Performing partial verification')
            return files_are_roughly_equal(source, target)
