from datetime import timedelta
import backy.utils
import datetime
import logging

logger = logging.getLogger(__name__)

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def parse_duration(duration):
    if duration.endswith('d'):
        duration = duration.replace('d', '')
        duration = int(duration) * DAY
    elif duration.endswith('h'):
        duration = duration.replace('h', '')
        duration = int(duration) * HOUR
    elif duration.endswith('m'):
        duration = duration.replace('m', '')
        duration = int(duration) * MINUTE
    elif duration.endswith('s'):
        duration = duration.replace('s', '')
        duration = int(duration)
    else:
        duration = int(duration)
    return timedelta(seconds=duration)


def next_in_interval(relative, interval, spread):
    relative = (relative - backy.utils.min_date()).total_seconds()
    interval = interval.total_seconds()
    # Ensure the spread fits the interval so we don't accidentally
    # jump one when adding the spread.
    spread = spread % interval
    # To avoid that we jump an interval while we're in the spread period,
    # we need to subtract it here. This is basically the reversal of
    # the + spread when we add it to the next interval.
    relative = relative - spread
    current_interval = int(relative / interval)
    next = (current_interval + 1) * interval + spread
    next = backy.utils.min_date() + datetime.timedelta(seconds=next)
    return next


class Schedule(object):

    def __init__(self):
        self.schedule = {}

    def configure(self, config):
        self.schedule = config
        for tag, spec in self.schedule.items():
            self.schedule[tag]['interval'] = parse_duration(spec['interval'])

    def next(self, relative, spread, archive):
        catchup = self._next_catchup(relative, spread, archive)
        if catchup[1]:
            return catchup
        else:
            return self._next_ideal(relative, spread)

    def _next_ideal(self, relative, spread):
        next_times = {}
        for tag, settings in self.schedule.items():
            t = next_times.setdefault(
                next_in_interval(relative, settings['interval'], spread),
                set())
            t.add(tag)
        next_time = min(next_times.keys())
        next_tags = next_times[next_time]
        return next_time, next_tags

    def _next_catchup(self, relative, spread, archive):
        # Catch up based on the spread within the next
        # 15 minutes.
        now = backy.utils.now()
        catchup_tags = set(self.schedule.keys())
        archive.scan()
        for tag, last in archive.last_by_tag().items():
            if tag not in self.schedule:
                # Ignore ad-hoc tags for catching up.
                continue
            if last > now - self.schedule[tag]['interval']:
                # We had a valid backup within the interval
                catchup_tags.remove(tag)
            if last < now - self.schedule[tag]['interval'] * 1.5:
                # Our last valid backup is almost due, we have passed
                # the 50% mark to the next backup and won't do a
                catchup_tags.remove(tag)
        # Catchups always happen immediately. There is no point in waiting as
        # we already have missed originally necessary backups due to
        # shutdown of the server or such.
        return (backy.utils.now(), catchup_tags)

    def expire(self, archive):
        """Remove old revisions according to the backup schedule.

        Returns list of removed revisions.
        """
        archive.scan()
        removed = []
        # Clean out old backups: keep at least a certain number of copies
        # (keep) and ensure that we don't throw away copies that are newer
        # than keep * interval for this tag.
        # Phase 1: remove tags that are expired
        for tag, args in self.schedule.items():
            revisions = archive.find_revisions('tag:' + tag)
            keep = args['keep']
            if len(revisions) < keep:
                continue
            keep_threshold = backy.utils.now() - keep * args['interval']
            for old_revision in revisions[:-keep]:
                if old_revision.timestamp >= keep_threshold:
                    continue
                old_revision.tags.remove(tag)
                old_revision.write_info()

        # Phase 2: delete revisions that have no tags any more.
        # We are deleting items of the history while iterating over it.
        # Use a copy of the list!
        for revision in list(archive.history):
            if revision.tags:
                continue
            removed.append(revision)
            revision.remove()

        return removed

    def sorted_tags(self, tags):
        """Return a list of tags, sorted by their interval. Smallest first."""
        t = {}
        for tag in tags:
            t[tag] = self.schedule.get(
                tag, {'interval': datetime.timedelta(0)})['interval']
        vals = t.items()
        return (x[0] for x in sorted(vals, key=lambda x: x[1]))
