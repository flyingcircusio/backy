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
        self._by_uuid = {}

    def scan(self):
        self.history = []
        self._by_uuid = {}
        for f in glob.glob(p.join(self.path, '*.rev')):
            if f.endswith('last.rev'):
                continue
            r = Revision.load(f, self)
            if r.uuid not in self._by_uuid:
                self._by_uuid[r.uuid] = r
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
            result = [self[spec]]
        return result

    def find_by_number(self, spec):
        """Returns revision by relative number from the end.

        Raises IndexError or ValueError if no revision is found.
        """
        spec = int(spec)
        if spec < 0:
            raise KeyError('Integer revisions must be positive')
        return self.history[-spec - 1]

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

    def __getitem__(self, spec):
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

    @property
    def clean_history(self):
        """History without incomplete revisions."""
        return [rev for rev in self.history if 'duration' in rev.stats]
