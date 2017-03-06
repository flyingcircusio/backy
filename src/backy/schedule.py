from datetime import timedelta
import backy.utils
import datetime
import logging

logger = logging.getLogger(__name__)

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
WEEK = 7 * DAY


def parse_duration(duration):
    if duration.endswith('w'):
        duration = duration.replace('w', '')
        duration = int(duration) * WEEK
    elif duration.endswith('d'):
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
        time, tags = ideal_time, ideal_tags = self._next_ideal(
            relative, spread)
        missed_tags = self._missed(archive)
        # The next run will include all missed tags
        tags.update(missed_tags)
        if missed_tags and len(archive.history):
            # Perform an immediate backup if we have any history at all.
            # and when we aren't running a regular backup within the next
            # 5 minutes anyway.
            grace_period = datetime.timedelta(seconds=5 * 60)
            if ideal_time > backy.utils.now() + grace_period:
                time = backy.utils.now()
                tags = missed_tags
        return time, tags

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

    def _missed(self, archive):
        # Check whether we missed any
        now = backy.utils.now()
        missing_tags = set(self.schedule.keys())
        for tag, last in archive.last_by_tag().items():
            if tag not in self.schedule:
                # Ignore ad-hoc tags for catching up.
                continue
            if last > now - self.schedule[tag]['interval']:
                # We had a valid backup within the interval
                missing_tags.remove(tag)
        return missing_tags

    def expire(self, backup):
        """Remove old revisions according to the backup schedule.

        Returns list of removed revisions.
        """
        backup.scan()
        removed = []
        # Clean out old backups: keep at least a certain number of copies
        # (keep) and ensure that we don't throw away copies that are newer
        # than keep * interval for this tag.
        # Phase 1: remove tags that are expired
        for tag, args in self.schedule.items():
            revisions = backup.find_revisions('tag:' + tag)
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
        for revision in list(backup.history):
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
