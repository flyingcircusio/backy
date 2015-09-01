from backy.revision import Revision
from backy.sources import select_source
from backy.utils import SafeFile, format_bytes_flexible, safe_copy
from backy.utils import format_timestamp
from glob import glob
from prettytable import PrettyTable
import errno
import fcntl
import logging
import os
import os.path
import time
import yaml


logger = logging.getLogger(__name__)


class Commands(object):
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self._backup = Backup(path)

    def init(self, type, source):
        self._backup.init(type, source)

    def status(self):
        self._backup._configure()
        total_bytes = 0

        t = PrettyTable(["Date", "ID", "Size", "Duration", "Tags"])
        t.align = 'l'
        t.align['Size'] = 'r'
        t.align['Duration'] = 'r'

        for r in self._backup.archive.history:
            total_bytes += r.stats.get('bytes_written', 0)
            t.add_row([format_timestamp(r.timestamp),
                       r.uuid,
                       format_bytes_flexible(r.stats.get('bytes_written', 0)),
                       int(r.stats.get('duration', 0)),
                       ', '.join(r.tags)])

        print(t)

        print("== Summary")
        print("{} revisions".format(len(self._backup.archive.history)))
        print("{} data (estimated)".format(
            format_bytes_flexible(total_bytes)))

    def backup(self, tags):
        self._backup._configure()
        try:
            self._backup.backup(tags)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            logger.info('Backup already in progress.')

    def restore(self, revision, target):
        self._backup._configure()
        self._backup.restore(revision, target)


class Archive(object):
    """Keeps track of existing revisions in a backup."""

    def __init__(self, path):
        self.path = os.path.realpath(path)
        self.history = []

    def scan(self):
        self.history = []
        seen_uuids = set()
        for file in glob(self.path + '/*.rev'):
            r = Revision.load(file, self)
            if r.uuid in seen_uuids:
                continue
            seen_uuids.add(r.uuid)
            self.history.append(r)
        self.history.sort(key=lambda r: r.timestamp)

    def last_by_tag(self):
        """Return a dictionary showing the last time each tag was
        backed up.

        Tags that have never been backed up won't show up here.

        """
        last_times = {}
        for revision in self.history:
            for tag in revision.tags:
                last_times.setdefault(tag, 0)
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
            result = [self.find_revision(spec)]
        return result

    def find_revision(self, spec):
        if spec is None:
            raise KeyError(spec)

        if spec in ['last', 'latest']:
            spec = 0

        try:
            spec = int(spec)
            if spec < 0:
                raise KeyError("Integer revisions must be positive.")
        except ValueError:
            # spec is a string and represents a UUID
            for r in self.history:
                if r.uuid == spec:
                    return r
            else:
                raise KeyError(spec)
        else:
            try:
                return self.history[-spec - 1]
            except IndexError:
                raise KeyError(spec)


class Backup(object):

    config = None

    def __init__(self, path):
        self.path = os.path.realpath(path)
        self.archive = Archive(self.path)
        logger.debug('Backup("{}")'.format(self.path))

    def _configure(self):
        if self.config is not None:
            return

        f = open(self.path + '/config', 'r', encoding='utf-8')
        self.config = yaml.load(f)

        try:
            source_factory = select_source(self.config['type'])
        except IndexError:
            logger.error("No source type named `{}` exists.".format(
                self.config['type']))
        self.source = source_factory(self.config)
        self.archive.scan()

    def _lock(self):
        self._lock_file = open(self.path + '/config', 'rb')
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def init(self, type, source):
        # Allow re-configuration in this case.
        if self.config:
            raise RuntimeError("Can not initialize configured backup objects.")
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if os.path.exists(self.path + '/config'):
            raise RuntimeError('Refusing to initialize with existing config.')

        source_factory = select_source(type)
        source_config = source_factory.config_from_cli(source)
        source_config['type'] = type

        with SafeFile(self.path + '/config', encoding='utf-8') as f:
            f.open_new('wb')
            d = yaml.dump(source_config)
            f.write(d)

    def backup(self, tags):
        self._lock()
        self.archive.scan()

        start = time.time()

        # Clean-up incomplete revisions
        for revision in self.archive.history:
            if 'duration' not in revision.stats:
                logger.info('Removing incomplete revision {}'.
                            format(revision.uuid))
                revision.remove()

        tags = set(tags.split(','))

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
                new_revision.defrag()
                new_revision.set_link('last')
                new_revision.stats['duration'] = time.time() - start
                new_revision.write_info()
                new_revision.readonly()
                self.archive.history.append(new_revision)

    def restore(self, revision, target):
        self._lock()
        self.archive.scan()

        r = self.archive.find_revision(revision)
        source = open(r.filename, 'rb')
        target = open(target, 'wb')
        safe_copy(source, target)
