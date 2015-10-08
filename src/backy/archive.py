from backy.revision import Revision
from backy.utils import min_date
import glob
import logging
import os.path as p

logger = logging.getLogger(__name__)


class Archive(object):
    """Keeps track of existing revisions in a backup."""

    def __init__(self, path):
        self.path = p.realpath(path)
        self.history = []

    def scan(self):
        self.history = []
        seen_uuids = set()
        for file in glob.glob(p.join(self.path, '*.rev')):
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

    @property
    def clean_history(self):
        """History without incomplete revisions."""
        return [rev for rev in self.history if 'duration' in rev.stats]
