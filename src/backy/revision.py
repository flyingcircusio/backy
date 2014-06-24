from backy.utils import SafeFile
import glob
import json
import logging
import os
import subprocess
import uuid

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

    def __init__(self, uuid, backup):
        self.uuid = uuid
        self.backup = backup
        self.stats = {'bytes_written': 0}

    @classmethod
    def create(cls, backup):
        r = Revision(str(uuid.uuid1()), backup)
        r.timestamp = backup.now()
        if backup.revision_history:
            # XXX validate that this parent is a actually a good parent. need
            # to contact the source for this ...
            r.parent = backup.revision_history[-1].uuid
        return r

    @classmethod
    def load(cls, file, backup):
        metadata = json.load(open(file, 'r', encoding='utf-8'))
        r = Revision(metadata['uuid'], backup)
        r.timestamp = metadata['timestamp']
        r.parent = metadata['parent']
        r.stats = metadata.get('stats', {})
        r.tags = metadata.get('tags', '')
        # XXX turn into a better "upgrade" method and start versioning
        # data files.
        if 'tag' in metadata:
            r.tags = [metadata['tag']]
        return r

    @property
    def filename(self):
        return '{}/{}'.format(self.backup.path, self.uuid)

    @property
    def info_filename(self):
        return self.filename + '.rev'

    def materialize(self):
        self.write_info()
        if not self.parent:
            open(self.filename, 'wb').close()
            return

        parent = self.backup.find_revision(self.parent)
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
            f.write(json.dumps(metadata))

    def set_link(self, name):
        path = self.backup.path
        if os.path.exists(path+'/'+name):
            os.unlink(path+'/'+name)
        os.symlink(os.path.relpath(self.filename, path), path+'/'+name)

        name = name + '.rev'
        if os.path.exists(path+'/'+name):
            os.unlink(path+'/'+name)
        os.symlink(os.path.relpath(self.info_filename, path), path+'/'+name)

    def remove(self, simulate=False):
        if not simulate:
            for file in glob.glob(self.filename+'*'):
                os.unlink(file)
        if self in self.backup.revision_history:
            self.backup.revision_history.remove(self)

    def writable(self):
        os.chmod(self.filename, 0o640)
        os.chmod(self.info_filename, 0o640)

    def readonly(self):
        os.chmod(self.filename, 0o440)
        os.chmod(self.info_filename, 0o440)
