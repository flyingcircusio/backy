from backy.utils import SafeFile
import backy.utils
import datetime
import glob
import logging
import os
import pytz
import shortuuid
import subprocess
import yaml


logger = logging.getLogger(__name__)
cmd = subprocess.check_output


def cp_reflink(source, target):
    # We can't tell if reflink is really supported. It depends on the
    # filesystem.
    try:
        with open('/dev/null', 'wb') as devnull:
            cmd(['cp', '--reflink=always', source, target], stderr=devnull)
    except subprocess.CalledProcessError:
        logger.warn('Performing non-COW copy: {} -> {}'.format(source, target))
        if os.path.exists(target):
            os.unlink(target)
        cmd(['cp', source, target])


class Revision(object):

    uuid = None
    timestamp = None
    parent = None
    stats = None
    tags = ()

    def __init__(self, uuid, archive):
        self.uuid = uuid
        self.archive = archive
        self.stats = {'bytes_written': 0}

    @classmethod
    def create(cls, archive):
        r = Revision(shortuuid.uuid(), archive)
        r.timestamp = backy.utils.now()
        if archive.history:
            # XXX validate that this parent is a actually a good parent. need
            # to contact the source for this ...
            r.parent = archive.history[-1].uuid
        return r

    @classmethod
    def load(cls, file, archive):
        with open(file, 'r', encoding='utf-8') as f:
            metadata = yaml.load(f)
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
        return '{}/{}'.format(self.archive.path, self.uuid)

    @property
    def info_filename(self):
        return self.filename + '.rev'

    def materialize(self):
        self.write_info()
        if not self.parent:
            open(self.filename, 'wb').close()
            return

        parent = self.archive.find_revision(self.parent)
        cp_reflink(parent.filename, self.filename)
        self.writable()

    def defrag(self):
        try:
            cmd(['btrfs', '--help'])
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warn('btrfs not found.')
            return
        try:
            cmd('btrfs filesystem defragment {}/*'.format(
                os.path.dirname(self.filename)), shell=True)
        except subprocess.CalledProcessError:
            logger.warn('Could not btrfs defrag. Is this a btrfs volume?')

    def write_info(self):
        metadata = {
            'uuid': self.uuid,
            'timestamp': self.timestamp,
            'parent': self.parent,
            'stats': self.stats,
            'tags': list(self.tags)}
        with SafeFile(self.info_filename, encoding='utf-8') as f:
            f.open_new('wb')
            f.write(yaml.dump(metadata))

    def set_link(self, name):
        path = self.archive.path
        if os.path.exists(path + '/' + name):
            os.unlink(path + '/' + name)
        os.symlink(os.path.relpath(self.filename, path), path + '/' + name)

        name = name + '.rev'
        if os.path.exists(path + '/' + name):
            os.unlink(path + '/' + name)
        os.symlink(os.path.relpath(self.info_filename, path),
                   path + '/' + name)

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
