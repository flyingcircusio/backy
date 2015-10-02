from backy.revision import Revision
from backy.sources import select_source
from backy.utils import min_date
from backy.utils import SafeFile, safe_copy
from glob import glob
import fcntl
import logging
import os
import os.path
import time
import yaml

logger = logging.getLogger(__name__)


class Archive(object):
    """Keeps track of existing revisions in a backup."""

    def __init__(self, path):
        self.path = os.path.realpath(path)
        self.history = []

    def scan(self):
        self.history = []
        seen_uuids = set()
        for file in glob(os.path.join(self.path, '*.rev')):
            r = Revision.load(file, self)
            if r.uuid not in seen_uuids:
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

    def configure(self):
        if self.config is not None:
            return

        with open(os.path.join(self.path, 'config'), encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
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
            yaml.safe_dump(source_config, f)

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
