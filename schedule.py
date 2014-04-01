import time
import datetime

now = time.time()

days = 24*60*60

tag_none_marker = object()


class Backup(object):

    def __init__(self, schedule):
        self.revisions = {}
        self.id = 0
        self.schedule = schedule

    def history(self, tag=tag_none_marker):
        history = [
            r for r in self.revisions.values()
            if tag is tag_none_marker or tag == r['tag']]
        history.sort(key=lambda r: r['timestamp'])
        return history

    def add(self, r):
        self.revisions[r['uuid']] = r

    def remove(self, r):
        del self.revisions[r['uuid']]

    def backup(self):
        newest = None
        # Scheduling integration: only create a new backup if the current
        # newest backup has left the specified interval.
        if self.revisions:
            newest = self.history()[-1]
            newest_age = now-newest['timestamp']
            new_interval = self.schedule.get(None)['interval']
            if newest_age < new_interval:
                return

        # Simulate a general backup run. Append a new full to history,
        # turn the last full into a diff and setup the parent pointer.
        uuid = hex(self.id).replace('0x', '').rjust(2, '0')
        self.id += 1
        if newest:
            newest['parent'] = uuid
            newest['type'] = '+'

        next = {"uuid": uuid,
                "tag": None,
                "timestamp": now,
                "type": "*"}
        self.add(next)

    def maintenance(self):
        # Generate new intermediate levels
        if len(self.revisions) >= 2:
            for tag, args in sorted(self.schedule.items(),
                                    key=lambda x: x[1]['interval']):
                if tag is None:
                    # The None level is used for generating backups
                    continue
                level_history = self.history(tag=tag)
                if level_history:
                    newest_level = level_history[-1]
                    newest_age = now - newest_level['timestamp']
                    if newest_age < args['interval']:
                        continue
                # turn the last n-1 backup into this
                # we except the most current backup as this is a full one that
                # will be turned into a diff on the next occasion and we do
                # not want to interfere with that
                revision = self.history()[-2]
                revision['tag'] = tag
                revision['type'] = args['type']

        # Clean out old backups
        for tag, args in self.schedule.items():
            for revision in self.history(tag=tag):
                age = now - revision['timestamp']
                if age > args['keep']:
                    self.remove(revision)

schedule = {
    None: {'interval': 1*days,
           'keep': 8*days},
    'weekly': {'interval': 7*days,
               'keep': 21*days,
               'type': '~'},
    'monthly': {'interval': 30*days,
                'keep': 90*days,
                'type': 'F'}}


def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %a %H:%M:%S")


#import curses
#curses.initscr()

backup = Backup(schedule)

start_time = now
while now - start_time < 120*days:
    now += 24*60*60
    backup.backup()
    backup.maintenance()
    time.sleep(0.5)

    print "="*80
    for r in backup.history():
        time_readable = format_timestamp(r['timestamp'])
        print "{1} | {0[type]} | {0[tag]} ".format(r, time_readable)
