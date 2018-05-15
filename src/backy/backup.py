from .backends.chunked import ChunkedFileBackend
from .backends.cowfile import COWFileBackend
from .nbd.server import Server
from .revision import Revision, TRUST_DISTRUSTED
from .sources import select_source
from .utils import SafeFile, CHUNK_SIZE, posix_fadvise, copy
from backy.utils import min_date
import fcntl
import glob
import logging
import os
import os.path as p
import time
import yaml


logger = logging.getLogger(__name__)

# Locking strategy:
#
# - You can only run one backup of a machine at a time, as the backup will
#   interact with this machines' list of snapshots and will get confused
#   if run in parallel.
# - You can restore/nbd while a backup is running.
# - You can only purge while nothing else is happening.
# - Trying to get a shared lock (specifically purge) will block and wait
#   whereas trying to get an exclusive lock (running backups, purging) will
#   immediately give up.
# - Locking is not re-entrant. It's forbidden and protected to call another
#   locking main function.


def locked(target=None, mode=None):
    if mode == 'shared':
        mode = fcntl.LOCK_SH
    elif mode == 'exclusive':
        mode = fcntl.LOCK_EX | fcntl.LOCK_NB
    else:
        raise ValueError("Unknown lock mode '{}'".format(mode))

    def wrap(f):
        def locked_function(self, *args, **kw):
            if target in self._lock_fds:
                raise RuntimeError('Bug: Locking is not re-entrant.')
            target_path = p.join(self.path, target)
            if not os.path.exists(target_path):
                open(target_path, 'wb').close()
            self._lock_fds[target] = os.open(target_path, os.O_RDONLY)
            try:
                fcntl.flock(self._lock_fds[target], mode)
            except BlockingIOError:
                print("Failed to get exclusive lock for '{}'. Continuing.".
                      format(f.__name__))
                return
            else:
                try:
                    return f(self, *args, **kw)
                finally:
                    fcntl.flock(self._lock_fds[target], fcntl.LOCK_UN)
            finally:
                del self._lock_fds[target]
        locked_function.__name__ = 'locked({}, {})'.format(f.__name__, target)
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

    @locked(target='.backup', mode='exclusive')
    def _clean(self):
        """Clean-up incomplete revisions."""
        for revision in self.history:
            if 'duration' not in revision.stats:
                logger.warning('Removing incomplete revision {}'.
                               format(revision.uuid))
                revision.remove()

    @locked(target='.backup', mode='exclusive')
    @locked(target='.purge', mode='shared')
    def backup(self, tags):
        start = time.time()

        new_revision = Revision.create(self, tags)
        new_revision.materialize()
        logger.info('New revision {} [{}]'.format(
                    new_revision.uuid, ','.join(new_revision.tags)))

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
            # Switched from a fine-grained syncing mechanism to "everything
            # once" when we're done. This is as safe but much faster.
            os.sync()

        # If there are distrusted revisions, then perform at least one
        # verification after a backup - for good measure and to keep things
        # moving along automatically. This could also be moved into the
        # scheduler.
        self.scan()
        for revision in reversed(self.clean_history):
            if revision.trust == TRUST_DISTRUSTED:
                logger.warning('Found inconsistent revision(s). '
                               'Performing single verification.')
                backend = self.backend_factory(revision)
                backend.verify()
                break

    def distrust(self, revision=None, from_=None, until=None):
        if revision:
            r = self.find(revision)
            r.distrust()
            r.write_info()
        else:
            for r in self.clean_history:
                if from_ and r.timestamp.date() < from_:
                    continue
                if until and r.timestamp.date() > until:
                    continue
                r.distrust()
                r.write_info()

    @locked(target='.purge', mode='exclusive')
    def verify(self, revision=None):
        if revision:
            r = self.find(revision)
            backend = self.backend_factory(r)
            backend.verify()
        else:
            for r in list(self.clean_history):
                if r.trust != TRUST_DISTRUSTED:
                    continue
                backend = self.backend_factory(r)
                backend.verify()

    @locked(target='.purge', mode='exclusive')
    def purge(self):
        backend = self.backend_factory(self.history[0])
        backend.purge()

    #################
    # Restoring

    # This needs no locking as it's only a wrapper for restore_file and
    # restore_stdout and locking isn't re-entrant.
    def restore(self, revision, target):
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

    @locked(target='.purge', mode='shared')
    def restore_file(self, source, target):
        """Bulk-copy from open revision `source` to target file."""
        logger.debug('Copying from "%s" to "%s"...', source.name, target)
        open(target, 'ab').close()  # touch into existence
        with open(target, 'r+b', buffering=CHUNK_SIZE) as target:
            try:
                posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            except Exception:
                pass
            copy(source, target)

    @locked(target='.purge', mode='shared')
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

    @locked(target='.purge', mode='shared')
    def nbd_server(self, host, port):
        server = Server((host, port), self)
        server.serve_forever()

    @locked(target='.purge', mode='shared')
    def upgrade(self):
        """Upgrade this backup's store from cowfile to chunked.

        This can take a long time and is intended to be interruptable.

        We start creating new backups with the new format once everything
        is converted as we do not want to interfere with the config file
        but allow upgrading without a format specification.

        """
        from backy.backends.chunked import ChunkedFileBackend
        from backy.sources.file import File

        last_worklist = []

        while True:
            self.scan()
            to_upgrade = [r for r in self.clean_history
                          if r.backend_type == 'cowfile']
            if not to_upgrade:
                break
            if to_upgrade == last_worklist:
                print("Not making progress, exiting.")
                break
            last_worklist = to_upgrade

            print("Found {} revisions to upgrade.".format(len(to_upgrade)))
            # Upgrade the newest then start again. The revisions may change
            # beneath us and this may cause a) new revisions to appear and b)
            # old revisions to disappear. We want to upgraded new revisions as
            # quickly as possible as having the newest upgraded means that
            # then next backup will be able to use the new format and we don't
            # have to re-upgrade it again.
            try:
                revision = to_upgrade[-1]
                print("Converting {} from {}".format(
                    revision.uuid, revision.timestamp))
                original_file = revision.filename + '.old'
                if not os.path.exists(original_file):
                    # We may be resuming a partial upgrade. Only move the file
                    # if our .old doesn't exist.
                    os.rename(revision.filename, original_file)
                else:
                    print("Resuming with existing .old")
                    if os.path.exists(revision.filename):
                        os.unlink(revision.filename)
                revision.writable()
                chunked = ChunkedFileBackend(revision)
                chunked.clone_parent = False
                cowfile = File(dict(filename=original_file,
                                    cow=False))(revision)
                # Keep a copy of the statistics as it will get replaced when
                # running the full copy.
                original_stats = revision.stats.copy()
                cowfile.backup(chunked)
                revision.stats = original_stats
                revision.backend_type = 'chunked'
                revision.write_info()
                revision.readonly()
                os.unlink(original_file)
            except Exception:
                logger.exception('Error upgrading revision')
                # We may be seeing revisions getting removed, try again.
                return

            # Wait a bit, to be graceful to the host system just in case this
            # truns into a spinning loop.
            time.sleep(5)

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
