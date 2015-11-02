from .archive import Archive
from .revision import Revision
from .sources import select_source
from .utils import SafeFile, safe_copy, CHUNK_SIZE, posix_fadvise
import fcntl
import logging
import os
import os.path as p
import time
import yaml

logger = logging.getLogger(__name__)


class Backup(object):
    """Access to backup images."""

    config = None
    _lock_file = None

    def __init__(self, path):
        self.path = p.realpath(path)
        self.archive = Archive(self.path)
        logger.debug('Backup("{}")'.format(self.path))

    def configure(self):
        if self.config is not None:
            return

        with open(p.join(self.path, 'config'), encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        try:
            source_factory = select_source(self.config['type'])
        except IndexError:
            logger.error("No source type named `{}` exists.".format(
                self.config['type']))
        self.source = source_factory(self.config)
        self.archive.scan()

    def _lock(self):
        self._lock_file = open(p.join(self.path, 'config'), 'rb')
        fcntl.flock(self._lock_file, fcntl.LOCK_EX)

    def init(self, type, source):
        # Allow re-configuration in this case.
        if self.config:
            raise RuntimeError("Can not initialize configured backup objects.")
        if not p.exists(self.path):
            os.makedirs(self.path)
        if p.exists(p.join(self.path, 'config')):
            raise RuntimeError('Refusing to initialize with existing config.')

        source_factory = select_source(type)
        source_config = source_factory.config_from_cli(source)
        source_config['type'] = type

        with SafeFile(p.join(self.path, 'config'), encoding='utf-8') as f:
            f.open_new('wb')
            yaml.safe_dump(source_config, f)

    def backup(self, tags):
        self._lock()
        start = time.time()

        # Clean-up incomplete revisions
        self.archive.scan()
        for revision in self.archive.history:
            if 'duration' not in revision.stats:
                logger.warning('Removing incomplete revision {}'.
                               format(revision.uuid))
                revision.remove()

        tags = set(t.strip() for t in tags.split(','))
        new_revision = Revision.create(self.archive)
        new_revision.tags = tags
        new_revision.materialize()

        logger.info('New revision {} [{}]'.format(
                    new_revision.uuid, ','.join(new_revision.tags)))

        with self.source(new_revision) as source:
            source.backup()
            if not source.verify():
                logger.error('New revision does not match source '
                             '- removing it.')
                new_revision.remove()
            else:
                logger.info('Revision {} verification OK.'.format(
                    new_revision.uuid))
                new_revision.set_link('last')
                new_revision.stats['duration'] = time.time() - start
                new_revision.write_info()
                new_revision.readonly()
                self.archive.history.append(new_revision)

    def restore_file(self, source, target):
        """Bulk-copy from open revision `source` to target file."""
        logger.debug('Copying from "%s" to "%s"...', source.name, target)
        t = open(target, 'wb', buffering=0)
        with t as target:
            posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            safe_copy(source, target)

    def restore_stdout(self, source):
        """Emit restore data to stdout (for pipe processing)."""
        logger.debug('Dumping from "%s" to stdout...', source.name)
        posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
        with os.fdopen(os.dup(1), 'wb') as target:
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                target.write(chunk)

    def restore(self, revision, target):
        self.archive.scan()
        r = self.archive[revision]
        with open(r.filename, 'rb', buffering=0) as source:
            if target != '-':
                logger.info('Restoring revision @ %s [%s]', r.timestamp,
                            ','.join(r.tags))
                self.restore_file(source, target)
            else:
                self.restore_stdout(source)

    def find(self, revision):
        """Locates `revision` and returns full path."""
        self.archive.scan()
        try:
            rev = self.archive[revision]
            return rev.filename
        except KeyError as e:
            raise RuntimeError('Cannot find revision in {}: {}'.format(
                self.path, str(e)))
