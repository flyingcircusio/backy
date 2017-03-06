from .backends.chunked import ChunkedFileBackend
from .backends.cowfile import COWFileBackend
from .revision import Revision
from .sources import select_source
from .utils import SafeFile, copy_overwrite, CHUNK_SIZE, posix_fadvise
from backy.utils import min_date
import fcntl
import glob
import logging
import os
import os.path as p
import time
import yaml


logger = logging.getLogger(__name__)


def locked(target='config'):
    def wrap(f):
        def locked_function(self, *args, **kw):
            if target not in self._lock_fds:
                target_path = p.join(self.path, target)
                if not os.path.exists(target_path):
                    open(target_path, 'wb').close()
                self._lock_fds[target] = os.open(target_path, os.O_RDONLY)
            fcntl.flock(self._lock_fds[target], fcntl.LOCK_EX)
            try:
                return f(self, *args, **kw)
            finally:
                fcntl.flock(self._lock_fds[target], fcntl.LOCK_UN)
        return locked_function
    return wrap


class Backup(object):
    """A backup of a VM.

    Provides access to methods to

    - backup, restore, and list revisions

    """

    config = None
    backend_type = None

    def __init__(self, path):
        self._lock_fds = {}

        self.path = p.realpath(path)
        self.scan()

        logger.debug('Backup("{}")'.format(self.path))

        # Load config from file
        with open(p.join(self.path, 'config'), encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # Initialize our source
        try:
            source_factory = select_source(self.config['type'])
        except IndexError:
            logger.error("No source type named `{}` exists.".format(
                self.config['type']))
        self.source = source_factory(self.config)

        # Initialize our backend
        self.backend_type = self.config.get('backend', None)
        if self.backend_type is None:
            if not self.history:
                # Start fresh backups with our new default.
                self.backend_type = 'chunked'
            else:
                # Choose to continue existing backups with whatever format
                # they are in.
                self.backend_type = self.history[-1].backend_type

        if self.backend_type == 'cowfile':
            self.backend_factory = COWFileBackend
        elif self.backend_type == 'chunked':
            self.backend_factory = ChunkedFileBackend

    @classmethod
    def init(cls, path, type, source):
        config_path = p.join(path, 'config')
        if p.exists(config_path):
            raise RuntimeError('Refusing to initialize with existing config.')

        if not p.exists(path):
            os.makedirs(path)

        source_factory = select_source(type)
        source_config = source_factory.config_from_cli(source)
        source_config['type'] = type

        with SafeFile(config_path, encoding='utf-8') as f:
            f.open_new('wb')
            yaml.safe_dump(source_config, f)

        return Backup(path)

    def scan(self):
        self.history = []
        self._by_uuid = {}
        for f in glob.glob(p.join(self.path, '*.rev')):
            if os.path.islink(f):
                # Ignore links that are used to create readable pointers
                continue
            r = Revision.load(f, self)
            if r.uuid not in self._by_uuid:
                self._by_uuid[r.uuid] = r
                self.history.append(r)
        # The history is stored: oldest first. newest last.
        self.history.sort(key=lambda r: r.timestamp)

    @property
    def clean_history(self):
        """History without incomplete revisions."""
        return [rev for rev in self.history if 'duration' in rev.stats]

    #################
    # Making backups

    @locked()
    def _clean(self):
        """Clean-up incomplete revisions."""
        for revision in self.history:
            if 'duration' not in revision.stats:
                logger.warning('Removing incomplete revision {}'.
                               format(revision.uuid))
                revision.remove()

    @locked()
    def backup(self, tags):
        start = time.time()

        self._clean()

        new_revision = Revision.create(self, tags)
        new_revision.materialize()
        logger.info('New revision {} [{}]'.format(
                    new_revision.uuid, ','.join(new_revision.tags)))

        if False:
            # XXX Helper for debugging, do not check in
            old = self.find(new_revision.parent)
            backend_old = self.backend_factory(old)
            source_old = self.source(old)
            with source_old as old_source:
                r = old_source.verify(backend_old)
                if not r:
                    logger.error('New revision does not match source '
                                 '- removing it.')
                    return

        # XXX
        backend = self.backend_factory(new_revision)
        with self.source(new_revision) as source:
            source.backup(backend)
            if not source.verify(backend):
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
                self.scan()

    @locked('.purge')
    def purge(self):
        self.scan()
        backend = self.backend_factory(self.history[0])
        backend.purge(self)

    #################
    # Restoring

    @locked('.purge')
    def restore(self, revision, target):
        self.scan()
        r = self.find(revision)
        backend = self.backend_factory(r)
        s = backend.open('rb')
        with s as source:
            if target != '-':
                logger.info('Restoring revision @ %s [%s]', r.timestamp,
                            ','.join(r.tags))
                self.restore_file(source, target)
            else:
                self.restore_stdout(source)

    @locked('.purge')
    def restore_file(self, source, target):
        """Bulk-copy from open revision `source` to target file."""
        logger.debug('Copying from "%s" to "%s"...', source.name, target)
        open(target, 'ab').close()  # touch into existence
        with open(target, 'r+b', buffering=CHUNK_SIZE) as target:
            try:
                posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            except Exception:
                pass
            copy_overwrite(source, target)

    @locked('.purge')
    def restore_stdout(self, source):
        """Emit restore data to stdout (for pipe processing)."""
        logger.debug('Dumping from "%s" to stdout...', source.name)
        try:
            posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
        except Exception:
            pass
        with os.fdopen(os.dup(1), 'wb') as target:
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                target.write(chunk)

    @locked('.purge')
    def nbd_server(self):
        pass

        self.purge()

    ######################
    # Looking up revisions

    def last_by_tag(self):
        """Return a dictionary showing the last time each tag was
        backed up.

        Tags that have never been backed up won't show up here.

        """
        last_times = {}
        for revision in self.history:
            for tag in revision.tags:
                last_times.setdefault(tag, min_date())
                last_times[tag] = max([last_times[tag], revision.timestamp])
        return last_times

    def find_revisions(self, spec):
        """Get a sorted list of revisions, oldest first, that match the given
        specification.
        """
        if isinstance(spec, str) and spec.startswith('tag:'):
            tag = spec.replace('tag:', '')
            result = [r for r in self.history
                      if tag in r.tags]
        elif spec == 'all':
            result = self.history[:]
        else:
            result = [self.find(spec)]
        return result

    def find_by_number(self, spec):
        """Returns revision by relative number.

        0 is the newest,
        1 is the next older,
        2 is the even next older,
        and so on ...

        Raises IndexError or ValueError if no revision is found.
        """
        spec = int(spec)
        if spec < 0:
            raise KeyError('Integer revisions must be positive')
        return self.history[-spec-1]

    def find_by_tag(self, spec):
        """Returns the latest revision matching a given tag.

        Raises IndexError or ValueError if no revision is found.
        """
        if spec in ['last', 'latest']:
            return self.history[-1]
        matching = [r for r in self.history if spec in r.tags]
        return max((r.timestamp, r) for r in matching)[1]

    def find_by_uuid(self, spec):
        """Returns revision matched by UUID.

        Raises IndexError if no revision is found.
        """
        try:
            return self._by_uuid[spec]
        except KeyError:
            raise IndexError()

    def find(self, spec):
        """Flexible revision search.

        Locates a revision by relative number, by tag, or by uuid.

        """
        if spec is None or spec == '' or not self.history:
            raise KeyError(spec)

        for find in (self.find_by_number, self.find_by_uuid, self.find_by_tag):
            try:
                return find(spec)
            except (ValueError, IndexError):
                pass
        raise KeyError(spec)
