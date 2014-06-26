from backy.revision import Revision
from backy.schedule import simulate, Schedule
from backy.sources import select_source
from backy.utils import SafeFile, format_bytes_flexible, safe_copy
from glob import glob
from prettytable import PrettyTable
import datetime
import errno
import fcntl
import json
import logging
import os
import os.path
import time


logger = logging.getLogger(__name__)


def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


class Commands(object):
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self._backup = Backup(path)

    def init(self, type, source):
        self._backup.init(type, source)

    def status(self):
        total_bytes = 0

        t = PrettyTable(["Date", "ID", "Size", "Duration", "Tags"])
        t.align = 'l'
        t.align['Size'] = 'r'
        t.align['Duration'] = 'r'

        for r in self._backup.revision_history:
            total_bytes += r.stats.get('bytes_written', 0)
            t.add_row([format_timestamp(r.timestamp),
                       r.uuid,
                       format_bytes_flexible(r.stats.get('bytes_written', 0)),
                       int(r.stats.get('duration', 0)),
                       ', '.join(r.tags)])

        print(t)

        print("== Summary")
        print("{} revisions".format(len(self._backup.revision_history)))
        print("{} data (estimated)".format(
            format_bytes_flexible(total_bytes)))

    def backup(self, force=False):
        try:
            self._backup.backup(force)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            logger.info('Backup already in progress.')

    def restore(self, revision, target):
        self._backup.restore(revision, target)

    def schedule(self, days):
        simulate(self._backup, days)


class Backup(object):

    config = None
    # Allow overriding in tests and simulation mode.
    now = time.time

    def __init__(self, path):
        self.path = os.path.realpath(path)
        logger.debug('Backup("{}")'.format(self.path))
        self._configure()
        self._scan_revisions()

    def _configure(self):
        if self.config is not None:
            raise RuntimeError(
                "Can not configure backup objects multiple times.")
        self.config = {}
        if not os.path.exists(self.path + '/config'):
            return
        self.config = json.load(
            open(self.path + '/config', 'r', encoding='utf-8'))
        self.source = select_source(
            self.config['source-type'])(self.config['source'])
        self.schedule = Schedule(self)

    def _scan_revisions(self):
        self.revision_history = []
        seen_uuids = set()
        for file in glob(self.path + '/*.rev'):
            r = Revision.load(file, self)
            if r.uuid in seen_uuids:
                continue
            seen_uuids.add(r.uuid)
            self.revision_history.append(r)
        self.revision_history.sort(key=lambda r: r.timestamp)

    def _lock(self):
        self._lock_file = open(self.path+'/config', 'rb')
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    # Internal API

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
            for r in self.revision_history:
                if r.uuid == spec:
                    return r
            else:
                raise KeyError(spec)
        else:
            try:
                return self.revision_history[-spec-1]
            except IndexError:
                raise KeyError(spec)

    def find_revisions(self, spec):
        """Get a sorted list of revisions, oldest first, that match the given
        specification.
        """
        if isinstance(spec, str) and spec.startswith('tag:'):
            tag = spec.replace('tag:', '')
            result = [r for r in self.revision_history
                      if tag in r.tags]
        elif spec == 'all':
            result = self.revision_history[:]
        else:
            result = [self.find_revision(spec)]
        return result

    def init(self, type, source):
        # Allow re-configuration in this case.
        if self.config != {}:
            raise RuntimeError("Can not initialize configured backup objects.")
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if os.path.exists(self.path + '/config'):
            raise RuntimeError('Refusing to initialize with existing config.')

        source_factory = select_source(type)
        source_config = source_factory.config_from_cli(source)

        with SafeFile(self.path+'/config', encoding='utf-8') as f:
            f.open_new('wb')
            d = json.dumps({'source': source_config,
                            'source-type': type,
                            'schedule': {'daily': {'interval': '1d',
                                                   'keep': 9},
                                         'weekly': {'interval': '7d',
                                                    'keep': 5},
                                         'monthly': {'interval': '30d',
                                                     'keep': 4}}})
            f.write(d)

        # Allow re-configuring after initialization.
        self.config = None
        self._configure()

    def backup(self, force=''):
        self._lock()
        self._scan_revisions()

        start = self.now()

        # Clean-up incomplete revisions
        for revision in self.revision_history:
            if 'duration' not in revision.stats:
                logger.info('Removing incomplete revision {}'.
                            format(revision.uuid))
                revision.remove()

        tags = self.schedule.next_due()
        tags.update(filter(None, force.split(',')))
        if not tags:
            logger.warning('No backup due yet.')
            return

        new_revision = Revision.create(self)
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
                new_revision.stats['duration'] = self.now() - start
                new_revision.write_info()
                new_revision.readonly()
                self.revision_history.append(new_revision)

        logger.info('Expiring old revisions ...')
        self.schedule.expire()

    def restore(self, revision, target):
        self._lock()
        self._scan_revisions()

        r = self.find_revision(revision)
        source = open(r.filename, 'rb')
        target = open(target, 'wb')
        safe_copy(source, target)
