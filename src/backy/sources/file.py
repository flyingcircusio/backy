from backy.utils import copy, copy_overwrite, files_are_equal
import logging

logger = logging.getLogger(__name__)


class File(object):

    def __init__(self, config):
        self.filename = config['filename']
        self.cow = config.get('cow', True)

    @staticmethod
    def config_from_cli(spec):
        return dict(filename=spec)

    def __call__(self, revision):
        self.revision = revision
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        pass

    def backup(self, target):
        logger.info('Performing full backup')
        s = open(self.filename, 'rb')
        t = target.open('r+b')
        with s as source, t as target:
            if self.cow:
                logger.info("Using sparse copy")
                bytes = copy_overwrite(source, target)
            else:
                logger.info("Using full copy")
                bytes = copy(source, target)

        self.revision.stats['bytes_written'] = bytes

    def verify(self, target):
        logger.info('Performing full verification')
        s = open(self.filename, 'rb')
        t = target.open('rb')
        with s as source, t as target:
            return files_are_equal(source, target)
