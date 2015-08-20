from backy.revision import Revision
import curses
import datetime
import fractions
import functools
import logging
import time


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
    return duration


class Schedule(object):

    # Allow overriding in tests and simulation mode.
    now = time.time

    def __init__(self):
        self.schedule = {}

    @property
    def quant(self):
        """Return quantization for intervals."""
        return functools.reduce(
            fractions.gcd,
            [x['interval'] for x in self.schedule.values()])

    def configure(self, config):
        self.schedule = config
        for tag, spec in self.schedule.items():
            self.schedule[tag]['interval'] = parse_duration(spec['interval'])

    def next(self, relative, spread):
        next_times = {}
        for tag, settings in self.schedule.items():
            interval = settings['interval']
            t = next_times.setdefault(
                relative + interval - ((spread + relative) % interval), [])
            t.append(tag)
        next_time = list(sorted(next_times))[0]
        next_tags = next_times[next_time]
        return datetime.datetime.fromtimestamp(next_time), next_tags

    def next_due(self, archive):
        """Return a timestamp and a set of tags that are due next.
        """
        dues = set()
        tags = set()
        for tag, args in self.schedule.items():
            tag_history = archive.find_revisions('tag:{}'.format(tag))
            if not tag_history:
                # Never made a backup with this tag before, so it's due
                # by default.
                dues.add(self.now())
                tags.add(tag)
            else:
                last = tag_history[-1].timestamp
                next = last + args['interval']
                if next <= self.now():
                    tags.add(tag)
        return due, tags

    def expire(self, archive, simulate=False):
        """Remove old revisions according to the backup schedule.
        """
        # Clean out old backups: keep at least a certain number of copies
        # (keep) and ensure that we don't throw away copies that are newer
        # than keep * interval for this tag.
        # Phase 1: remove tags that are expired
        for tag, args in self.schedule.items():
            revisions = archive.find_revisions('tag:' + tag)
            keep = args['keep']
            if len(revisions) < keep:
                continue
            now = self.now()
            keep_threshold = now - keep * args['interval']
            for old_revision in revisions[:-keep]:
                if old_revision.timestamp >= keep_threshold:
                    continue
                old_revision.tags.remove(tag)

        # Phase 2: delete revisions that have no tags any more.
        for revision in archive.history:
            if not revision.tags:
                revision.remove(simulate)


def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %a %H:%M:%S")


def simulate(backup, days):
    curses.wrapper(simulate_main, backup.schedule, days)


def simulate_main(stdscr, schedule, days):
    stdscr.clear()
    stdscr.nodelay(True)

    schedule.backup.now = lambda: now

    now = time.time()

    header = curses.newwin(3, curses.COLS, 0, 0)
    header.addstr("SCHEDULE SIMULATION".center(curses.COLS-1, '='))
    header.refresh()

    revision_log = curses.newwin(20, curses.COLS, 3, 0)

    histogram = curses.newwin(10, 31, 23, 30)

    footer = curses.newwin(1, curses.COLS, curses.LINES-2, 0)

    days = days - 1
    start_time = now
    iteration = 0
    while now - start_time < days * DAY:
        now += 1*60*60
        iteration += 1

        revision_log.clear()

        # Simulate a new backup
        tags = schedule.next_due()
        if tags:
            r = Revision.create(schedule.backup)
            r.tags = list(tags)
            schedule.backup.archive.history.append(r)

        schedule.expire(simulate=True)

        revisions = schedule.backup.archive.history
        revision_log.addstr(
            " iteration {} | {} | {} revisions | estimated data: {} GiB\n\n"
            .format(iteration, format_timestamp(now), len(revisions), 0))

        for i, r in enumerate(revisions[-15:]):
            time_readable = format_timestamp(r.timestamp)
            revision_log.addstr(
                "{1} | {0.uuid} | {2:20s}\n".
                format(r, time_readable, ', '.join(r.tags)))

        revisions_by_day = {}
        for revision in schedule.backup.archive.history:
            revisions_by_day[
                datetime.date.fromtimestamp(revision.timestamp)] = revision
        hist_max = datetime.date.fromtimestamp(schedule.backup.now())
        hist_now = hist_max - datetime.timedelta(days=180)
        histogram.clear()
        hist_data = []
        while hist_now <= hist_max:
            r = revisions_by_day.get(hist_now, None)
            if r is None:
                ch = ' '
            else:
                ch = r.tags[0][0] if r.tags else '.'
            hist_data.append(ch)
            hist_now += datetime.timedelta(days=1)
        hist_data = ''.join(hist_data)
        hist_data = hist_data.rjust(10*31-1, '_')
        histogram.addstr(hist_data)
        histogram.refresh()
        revision_log.refresh()
        x = stdscr.getch()
        if x == ord('q'):
            break
        elif x == ord(' '):
            x = None
            while x != ord(' '):
                x = stdscr.getch()
        time.sleep(0.1)

    stdscr.nodelay(False)
    footer.addstr("DONE - PRESS KEY".center(curses.COLS-2, "="))
    footer.refresh()
    stdscr.getch()
