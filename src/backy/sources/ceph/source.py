from .rbd import RBDClient
from backy.revision import TRUST_DISTRUSTED
import backy.utils
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
        self._delete_old_snapshots()

    def backup(self, target):
        if self.always_full:
            logger.info('Full backup: per configuration')
            self.full(target)
            return
        try:
            parent = self.revision.backup.find(self.revision.parent)
            if parent.trust == TRUST_DISTRUSTED:
                raise KeyError('Full backup: distrusted parent')
            if not self.rbd.exists(self._image_name + '@backy-' + parent.uuid):
                raise KeyError(
                    'Full backup: could not find snapshot for '
                    'previous revision')
        except KeyError as e:
            logger.info(e.args[0])
            self.full(target)
            return
        self.diff(target)

    def diff(self, target):
        logger.info('Performing differential backup')
        snap_from = 'backy-' + self.revision.parent
        snap_to = 'backy-' + self.revision.uuid
        s = self.rbd.export_diff(
            self._image_name + '@' + snap_to, snap_from)
        t = target.open('r+b')
        with s as source, t as target:
            bytes = source.integrate(target, snap_from, snap_to)
        logger.info('Integration finished.')

        self.revision.stats['bytes_written'] = bytes

        # TMP Gather statistics to see where to optimize
        from backy.backends.chunked.chunk import chunk_stats
        self.revision.stats['chunk_stats'] = chunk_stats

    def full(self, target):
        logger.info('Performing full backup')
        s = self.rbd.export('{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid))
        t = target.open('r+b')
        copied = 0
        with s as source, t as target:
            while True:
                buf = source.read(4*backy.utils.MiB)
                if not buf:
                    break
                target.write(buf)
                copied += len(buf)
        self.revision.stats['bytes_written'] = copied

        # TMP Gather statistics to see if we actually are aligned.
        from backy.backends.chunked.chunk import chunk_stats
        self.revision.stats['chunk_stats'] = chunk_stats

    def verify(self, target):
        s = self.rbd.image_reader('{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid))
        t = target.open('rb')

        self.revision.stats['ceph-verification'] = 'partial'

        with s as source, t as target:
            logger.info('Performing partial verification')
            return backy.utils.files_are_roughly_equal(source, target)

    def _delete_old_snapshots(self):
        # Clean up all snapshots except the one for the most recent valid
        # revision.
        # Previously we used to remove all snapshots but the one for this
        # revision - which is wrong: broken new revisions would always cause
        # full backups instead of new deltas based on the most recent valid
        # one.
        if not self.always_full and self.revision.backup.history:
            keep_snapshot_revision = self.revision.backup.history[-1]
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
