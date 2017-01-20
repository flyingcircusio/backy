from .utils import SafeFile, now, safe_symlink, Remover
import datetime
import glob
import logging
import os
import os.path as p
import pytz
import shortuuid
import yaml


logger = logging.getLogger(__name__)


class Revision(object):

    uuid = None
    timestamp = None
    parent = None
    stats = None
    tags = ()

    def __init__(self, backup, uuid=None, timestamp=None):
        self.backup = backup
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
            'timestamp': self.timestamp,
            'parent': self.parent,
            'stats': self.stats,
            'tags': list(self.tags)}
        with SafeFile(self.info_filename, encoding='utf-8') as f:
            f.open_new('wb')
            yaml.safe_dump(metadata, f)

    def set_link(self, name):
        path = self.backup.path
        safe_symlink(self.filename, p.join(path, name))
        safe_symlink(self.info_filename, p.join(path, name + '.rev'))

    def remove(self):
        remover = Remover(glob.glob(self.filename + '*'))
        remover.start()
        self._remover = remover  # test support
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
            try:
                return self.backup.find_by_uuid(self.parent)
            except (KeyError, IndexError):
                pass
        return
