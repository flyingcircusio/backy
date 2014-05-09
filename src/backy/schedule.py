from backy.revision import Revision
import curses
import datetime
import time
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
    return duration


class Schedule(object):

    def __init__(self, backup):
        self.backup = backup
        self.schedule = self.backup.config.get('schedule', {})

    def next_due(self):
        """Time until the next backup (with tag '') is due (>0) or
        since it has been due (<0) in seconds.
        """
        if not self.backup.revision_history:
            return 0  # never backed up before
        last = self.backup.revision_history[-1].timestamp
        interval = self.schedule.get('', {}).get('interval', '1d')
        next_backup_due = last + parse_duration(interval)
        return next_backup_due - self.backup.now()

    def expire(self, simulate=False):
        """Remove old revisions according to the backup schedule.
        """
        if not self.backup.revision_history:
            return
        for tag, args in sorted(self.schedule.items(),
                                key=lambda x: parse_duration(x[1]['interval']),
                                reverse=True):
            if not tag:
                # The untagged level is used for generating backups
                continue
            level_history = self.backup.find_revisions('tag:'+tag)
            if level_history:
                newest_level = level_history[-1]
                newest_age = self.backup.now() - newest_level.timestamp
                if newest_age < parse_duration(args['interval']):
                    continue
            revision = self.backup.revision_history[-1]
            revision.tag = tag
            if level_history:
                # XXX i think this is superfluous by now
                previous = level_history[-1]
                previous.parent = revision.uuid
            break

        # Clean out old backups: keep at least a certain number of copies
        # (keep) and ensure that we don't throw away copies that are newer
        # than keep * interval for this tag.
        for tag, args in self.schedule.items():
            revisions = self.backup.find_revisions('tag:'+tag)
            keep = args['keep']
            if len(revisions) < keep:
                continue
            now = self.backup.now()
            for old_revision in revisions[:-keep]:
                if old_revision.timestamp > (
                        now - (keep * parse_duration(args['interval']))):
                    continue
                old_revision.remove(simulate)


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
        if schedule.next_due() <= 0:
            r = Revision.create(schedule.backup)
            schedule.backup.revision_history.append(r)

        schedule.expire(simulate=True)

        revisions = schedule.backup.revision_history
        revision_log.addstr(
            " iteration {} | {} | {} revisions | estimated data: {} GiB\n\n"
            .format(iteration, format_timestamp(now), len(revisions), 0))

        for i, r in enumerate(revisions[-15:]):
            time_readable = format_timestamp(r.timestamp)
            revision_log.addstr(
                "{1} | {0.uuid} | {0.tag:10s}\n".
                format(r, time_readable))

        revisions_by_day = {}
        for revision in schedule.backup.revision_history:
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
                ch = r.tag[0] if r.tag else '.'
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
