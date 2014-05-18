from .rbd import RBDClient
from backy.utils import safe_copy, SafeWritableFile, files_are_equal


class CephRBD(object):

    def __init__(self, config):
        self.pool = config['pool']
        self.image = config['image']
        self.rbd = RBDClient()

    @staticmethod
    def config_from_cli(spec):
        pool, image = spec.split('/')
        return dict(pool=pool, image=image)

    @property
    def snapshot_image(self):
        return '{}/{}@backy-{}'.format(
            self.pool, self.image, self.revision.uuid)

    def __call__(self, revision):
        self.revision = revision
        return self

    def __enter__(self):
        self.rbd.snap_create()
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        for snapshot in self.rbd.snap_ls():
            if not snapshot['name'].startswith('backy-'):
                continue
            uuid = snapshot['name'].replace('backy-', '')
            if uuid != self.revision.uuid:
                self.rbd.snap_rm(snapshot)

    def backup(self):
        if self.rbd.exists(self.revision.parent.snapshot_image):
            self.diff()
        else:
            self.full()

    def diff(self):
        d = self.rbd.export_diff()
        t = SafeWritableFile(self.revision.filename, rename=False, mode='r+b')
        with d as diff, t as target:
            bytes = diff.integrate(target)
        self.revision.stats['bytes_written'] = bytes

    def full(self):
        s = self.rbd.image_file(self.snapshot_image)
        t = open(self.revision.filename, 'r+b')
        with s as source, t as target:
            bytes = safe_copy(source, target)
        self.revision.stats['bytes_written'] = bytes

    def verify(self):
        s = self.rbd.image_file(self.snapshot_image)
        t = open(self.revision.filename, 'rb')
        with s as source, t as target:
            return files_are_equal(source, target)
