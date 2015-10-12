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
        return [r for r in self.history if r.uuid == spec][0]

    def __getitem__(self, spec):
        """Flexible revision search.

        Locates a revision by relative number, by tag, or by uuid.
        """
        if spec is None or spec == '':
            raise KeyError(spec)
        if not self.history:
            raise KeyError(spec)

        for selector in (self.find_by_number,
                         self.find_by_tag,
                         self.find_by_uuid):
            try:
                return selector(spec)
            except (ValueError, IndexError):
                pass
        raise KeyError(spec)

    @property
    def clean_history(self):
        """History without incomplete revisions."""
        return [rev for rev in self.history if 'duration' in rev.stats]
