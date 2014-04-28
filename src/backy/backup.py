from backy.revision import Revision
from backy.sources import select_source
from backy.utils import SafeWritableFile, format_bytes_flexible
from glob import glob
import datetime
import json
import os
import os.path
import time
import logging

logger = logging.getLogger(__name__)


def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


class Backup(object):

    INTERVAL = 24 * 60 * 60  # 1 day

    def __init__(self, path):
        # The path identifies the newest backup. Additional files
        # will be named with suffixes.
        self.path = os.path.realpath(path)
        logger.debug('Using backup located at {}'.format(self.path))
        self._configure()

    def _configure(self):
        if not os.path.exists(self.path + '/config'):
            return
        config = json.load(open(self.path + '/config', 'r', encoding='utf-8'))
        self.INTERVAL = config.get('interval', self.INTERVAL)

        self.source = select_source(config['source-type'])(config['source'])

    def _scan_revisions(self):
        self.revisions = {}
        # Load all revision infos
        for file in glob(self.path + '/*.rev'):
            r = Revision.load(file, self)
            self.revisions[r.uuid] = r

        self.revision_history = list(self.revisions.values())
        self.revision_history.sort(key=lambda r: r.timestamp)

    # Internal API

    def find_revision(self, spec):
        if spec == 'last':
            spec = 0

        try:
            spec = int(spec)
            if spec < 0:
                raise KeyError("Integer revisions must be positive.")
        except ValueError:
            return self.revisions[spec]
        else:
            try:
                return self.revision_history[-spec-1]
            except IndexError:
                raise KeyError(spec)

    def find_revisions(self, spec):
        if spec == 'all':
            result = self.revision_history[:]
        else:
            result = [self.find_revision(spec)]
        return result

    # Command API

    def init(self, type, source):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if os.path.exists(self.path + '/config'):
            raise RuntimeError('Refusing to initialize with existing config.')

        source_factory = select_source(type)
        source_config = source_factory.config_from_cli(source)

        with SafeWritableFile(self.path+'/config') as f:
            d = json.dumps({'source': source_config,
                            'source-type': type})
            d = d.encode('utf-8')
            f.write(d)
        self._configure()

    def status(self):
        self._scan_revisions()

        total_bytes = 0

        print("== Revisions")
        for r in self.revision_history:
            total_bytes += r.stats.get('bytes_written', 0)
            print("{0}\t{1}\t{2}\t{3:d}s".format(
                format_timestamp(r.timestamp),
                r.uuid,
                format_bytes_flexible(r.stats.get('bytes_written', 0)),
                int(r.stats.get('duration', 0))))

        print()
        print("== Summary")
        print("{} revisions".format(len(self.revisions)))
        print("{} data (estimated)".format(
            format_bytes_flexible(total_bytes)))

    def backup(self):
        self._scan_revisions()

        # Clean-up incomplete revisions
        for revision in self.revision_history:
            if 'duration' not in revision.stats:
                logger.info('Removing incomplete revision {}'.
                            format(revision.uuid))
                revision.remove()

        self._scan_revisions()

        start = time.time()

        new_revision = Revision.create(self)
        if self.revision_history:
            new_revision.parent = self.revision_history[-1].uuid
        new_revision.materialize()

        self.source.backup(new_revision)
        new_revision.set_link('last')
        new_revision.stats['duration'] = time.time() - start
        new_revision.write_info()

    def maintenance(self, keep):
        self._scan_revisions()
        for r in self.revision_history[:-keep]:
            print("Removing revision {}".format(r.uuid))
            r.remove()
