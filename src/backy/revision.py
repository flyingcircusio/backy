from .utils import SafeFile, now, safe_symlink, cp_reflink
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
    parent = None
    stats = None
    tags = ()

    def __init__(self, uuid, archive, timestamp=None):
        self.uuid = uuid
        self.archive = archive
        self.timestamp = timestamp
        self.stats = {'bytes_written': 0}

    @classmethod
    def create(cls, archive):
        r = Revision(shortuuid.uuid(), archive, now())
        if archive.history:
            # XXX validate that this parent is a actually a good parent. need
            # to contact the source for this ...
            r.parent = archive.history[-1].uuid
        return r

    @classmethod
    def load(cls, filename, archive):
        with open(filename, encoding='utf-8') as f:
            metadata = yaml.safe_load(f)
        r = Revision(metadata['uuid'], archive)
        if isinstance(metadata['timestamp'], float):
            metadata['timestamp'] = datetime.datetime.fromtimestamp(
                metadata['timestamp'])
        # PyYAML doesn't support round-trip for timezones. :(
        # http://pyyaml.org/ticket/202
        r.timestamp = pytz.UTC.localize(metadata['timestamp'])
        r.parent = metadata['parent']
        r.stats = metadata.get('stats', {})
        r.tags = set(metadata.get('tags', []))
        return r

    @property
    def filename(self):
        """Full pathname of the image file."""
        return os.path.join(self.archive.path, str(self.uuid))

    @property
    def info_filename(self):
        """Full pathname of the metadata file."""
        return self.filename + '.rev'

    def materialize(self):
        self.write_info()
        if not self.parent:
            open(self.filename, 'wb').close()
        else:
            parent = self.archive[self.parent]
            cp_reflink(parent.filename, self.filename)
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
        path = self.archive.path
        safe_symlink(self.filename, p.join(path, name))
        safe_symlink(self.info_filename, p.join(path, name + '.rev'))

    def remove(self):
        for file in glob.glob(self.filename + '*'):
            os.unlink(file)
        if self in self.archive.history:
            self.archive.history.remove(self)

    def writable(self):
        os.chmod(self.filename, 0o640)
        os.chmod(self.info_filename, 0o640)

    def readonly(self):
        os.chmod(self.filename, 0o440)
        os.chmod(self.info_filename, 0o440)
