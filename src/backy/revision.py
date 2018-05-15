from .utils import SafeFile, now, safe_symlink
import datetime
import glob
import logging
import os
import os.path as p
import pytz
import shortuuid
import yaml


TRUST_TRUSTED = 'trusted'
TRUST_DISTRUSTED = 'distrusted'
TRUST_VERIFIED = 'verified'


logger = logging.getLogger(__name__)


class Revision(object):

    uuid = None
    timestamp = None
    parent = None
    stats = None
    tags = ()
    trust = TRUST_TRUSTED  # or TRUST_DISTRUSTED, TRUST_VERIFIED

    def __init__(self, backup, uuid=None, timestamp=None):
        self.backup = backup
        self.backend_type = backup.backend_type
        self.uuid = uuid if uuid else shortuuid.uuid()
        self.timestamp = timestamp if timestamp else now()
        self.stats = {'bytes_written': 0}

    @classmethod
    def create(cls, backup, tags):
        r = Revision(backup)
        r.tags = tags
        if backup.history:
            r.parent = backup.history[-1].uuid
        return r

    @property
    def backend(self):
        return self.backup.backend_factory(self)

    def open(self, mode='rb'):
        return self.backend.open(mode)

    @classmethod
    def load(cls, filename, backup):
        with open(filename, encoding='utf-8') as f:
            metadata = yaml.safe_load(f)
        if isinstance(metadata['timestamp'], float):
            metadata['timestamp'] = datetime.datetime.fromtimestamp(
                metadata['timestamp'])
        # PyYAML doesn't support round-trip for timezones. :(
        # http://pyyaml.org/ticket/202
        r = Revision(backup,
                     uuid=metadata['uuid'],
                     timestamp=pytz.UTC.localize(metadata['timestamp']))
        r.parent = metadata['parent']
        r.stats = metadata.get('stats', {})
        r.tags = set(metadata.get('tags', []))
        # Assume trusted by default to support migration
        r.trust = metadata.get('trust', TRUST_TRUSTED)
        # If the metadata does not show the backend type, then it's cowfile.
        r.backend_type = metadata.get('backend_type', 'cowfile')
        return r

    @property
    def filename(self):
        """Full pathname of the image file."""
        return os.path.join(self.backup.path, str(self.uuid))

    @property
    def info_filename(self):
        """Full pathname of the metadata file."""
        return self.filename + '.rev'

    def materialize(self):
        self.write_info()
        self.writable()

    def write_info(self):
        metadata = {
            'uuid': self.uuid,
            'backend_type': self.backend_type,
            'timestamp': self.timestamp,
            'parent': self.parent,
            'stats': self.stats,
            'trust': self.trust,
            'tags': list(self.tags)}
        with SafeFile(self.info_filename, encoding='utf-8') as f:
            f.open_new('wb')
            yaml.safe_dump(metadata, f)

    def set_link(self, name):
        path = self.backup.path
        safe_symlink(self.filename, p.join(path, name))
        safe_symlink(self.info_filename, p.join(path, name + '.rev'))

    def distrust(self):
        logger.info('Distrusting revision %s', self.uuid)
        self.trust = TRUST_DISTRUSTED

    def verify(self):
        logger.info('Marking revision as verified %s', self.uuid)
        self.trust = TRUST_VERIFIED

    def remove(self):
        for filename in glob.glob(self.filename + '*'):
            if os.path.exists(filename):
                logger.info('Removing %s', filename)
                os.remove(filename)
                logger.info('Removed %s', filename)

        if self in self.backup.history:
            self.backup.history.remove(self)

    def writable(self):
        if os.path.exists(self.filename):
            os.chmod(self.filename, 0o640)
        os.chmod(self.info_filename, 0o640)

    def readonly(self):
        if os.path.exists(self.filename):
            os.chmod(self.filename, 0o440)
        os.chmod(self.info_filename, 0o440)

    def get_parent(self):
        if self.parent:
            return self.backup.find_by_uuid(self.parent)
        return
