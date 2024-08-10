import copy
import datetime
from datetime import timedelta
from typing import TYPE_CHECKING, Dict, Iterable, List, Set, Tuple

import backy.utils
from backy.revision import Revision, filter_schedule_tags

if TYPE_CHECKING:
    from backy.repository import Repository

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
WEEK = 7 * DAY


def parse_duration(duration):
    if duration.endswith("w"):
        duration = duration.replace("w", "")
        duration = int(duration) * WEEK
    elif duration.endswith("d"):
        duration = duration.replace("d", "")
        duration = int(duration) * DAY
    elif duration.endswith("h"):
        duration = duration.replace("h", "")
        duration = int(duration) * HOUR
    elif duration.endswith("m"):
        duration = duration.replace("m", "")
        duration = int(duration) * MINUTE
    elif duration.endswith("s"):
        duration = duration.replace("s", "")
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
    next = backy.utils.min_date() + timedelta(seconds=next)
    return next


class Schedule(object):
    schedule: dict
    config: dict

    def __init__(self):
        self.schedule = {}
        self.config = {}

    def configure(self, config: dict) -> None:
        self.config = config
        self.schedule = copy.deepcopy(config)
        for tag, spec in self.schedule.items():
            self.schedule[tag]["interval"] = parse_duration(spec["interval"])

    def to_dict(self) -> dict:
        return self.config

    @classmethod
    def from_dict(cls, conf) -> "Schedule":
        r = cls()
        r.configure(conf)
        return r

    def next(
        self, relative: datetime.datetime, spread: int, repository: "Repository"
    ) -> Tuple[datetime.datetime, Set[str]]:
        time, tags = ideal_time, ideal_tags = self._next_ideal(relative, spread)
        missed_tags = self._missed(repository)
        # The next run will include all missed tags
        tags.update(missed_tags)
        if missed_tags and len(repository.history):
            # Perform an immediate backup if we have any history at all.
            # and when we aren't running a regular backup within the next
            # 5 minutes anyway.
            grace_period = timedelta(seconds=5 * 60)
            if ideal_time > backy.utils.now() + grace_period:
                time = backy.utils.now()
                tags = missed_tags
        return time, tags

    def _next_ideal(
        self, relative: datetime.datetime, spread: int
    ) -> Tuple[datetime.datetime, Set[str]]:
        next_times: Dict[datetime.datetime, set] = {}
        for tag, settings in self.schedule.items():
            t = next_times.setdefault(
                next_in_interval(relative, settings["interval"], spread), set()
            )
            t.add(tag)
        next_time = min(next_times.keys())
        next_tags = next_times[next_time]
        return next_time, next_tags

    def _missed(self, repository: "Repository") -> Set[str]:
        # Check whether we missed any
        now = backy.utils.now()
        missing_tags = set(self.schedule.keys())
        for tag, last in repository.last_by_tag().items():
            if tag not in self.schedule:
                # Ignore ad-hoc tags for catching up.
                continue
            if last > now - self.schedule[tag]["interval"]:
                # We had a valid backup within the interval
                missing_tags.remove(tag)
        return missing_tags

    def expire(self, repository: "Repository") -> List["Revision"]:
        """Remove old revisions according to the backup schedule.

        Returns list of removed revisions.
        """
        repository.scan()
        removed = []
        # Clean out old backups: keep at least a certain number of copies
        # (keep) and ensure that we don't throw away copies that are newer
        # than keep * interval for this tag.
        # Phase 1: remove tags that are expired
        for tag, args in self.schedule.items():
            revisions = repository.find_revisions("tag:" + tag)
            keep = args["keep"]
            if len(revisions) < keep:
                continue
            keep_threshold = backy.utils.now() - keep * args["interval"]
            for old_revision in revisions[:-keep]:
                if old_revision.timestamp >= keep_threshold:
                    continue
                old_revision.tags.remove(tag)
                old_revision.write_info()

        # Phase 2: remove all tags which have been created by a former schedule
        for revision in repository.history:
            expired_tags = (
                filter_schedule_tags(revision.tags) - self.schedule.keys()
            )
            if expired_tags:
                revision.tags -= expired_tags
                revision.write_info()

        # Phase 3: delete revisions that have no tags any more.
        # We are deleting items of the history while iterating over it.
        # Use a copy of the list!
        for revision in list(repository.history):
            if revision.tags:
                continue
            removed.append(revision)
            revision.remove()

        return removed

    def sorted_tags(self, tags: Iterable[str]) -> Iterable[str]:
        """Return a list of tags, sorted by their interval. Smallest first."""
        t = {}
        for tag in tags:
            t[tag] = self.schedule.get(tag, {"interval": timedelta(0)})[
                "interval"
            ]
        vals = t.items()
        return (x[0] for x in sorted(vals, key=lambda x: x[1]))
