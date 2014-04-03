import time
import datetime

now = time.time()

days = 24*60*60

tag_none_marker = object()


class Revision(object):

    timestamp = None
    parent = None
    uuid = None
    tag = ''
    type = None

    def __init__(self, backup):
        self.backup = backup

    @property
    def parents(self):
        parent = self.parent
        while parent:
            r = self.backup.revisions[parent]
            yield r
            parent = r.parent


class Backup(object):

    def __init__(self, schedule):
        self.revisions = {}
        self.id = 0
        self.schedule = schedule

    def history(self, tag=tag_none_marker):
        history = [
            r for r in self.revisions.values()
            if tag is tag_none_marker or tag == r.tag]
        history.sort(key=lambda r: r.timestamp)
        return history

    def add(self, r):
        self.revisions[r.uuid] = r

    def remove(self, r):
        del self.revisions[r.uuid]

    def backup(self):
        newest = None
        # Scheduling integration: only create a new backup if the current
        # newest backup has left the specified interval.
        if self.revisions:
            newest = self.history()[-1]
            newest_age = now-newest.timestamp
            new_interval = self.schedule.get('')['interval']
            if newest_age < new_interval:
                return

        # Simulate a general backup run. Append a new full to history,
        # turn the last full into a diff and setup the parent pointer.
        uuid = hex(self.id).replace('0x', '').rjust(2, '0')
        self.id += 1
        if newest:
            newest.parent = uuid
            newest.type = '+'

        next = Revision(self)
        next.uuid = uuid
        next.timestamp = now
        next.type = 'F'
        self.add(next)

    def maintenance(self):
        # Generate new intermediate levels
        if len(self.revisions) >= 2:
            for tag, args in sorted(self.schedule.items(),
                                    key=lambda x: x[1]['interval']):
                if not tag:
                    # The untagged level is used for generating backups
                    continue
                level_history = self.history(tag=tag)
                if level_history:
                    newest_level = level_history[-1]
                    newest_age = now - newest_level.timestamp
                    if newest_age < args['interval']:
                        continue
                # turn the last n-1 backup into this
                # we except the most current backup as this is a full one that
                # will be turned into a diff on the next occasion and we do
                # not want to interfere with that
                revision = self.history()[-2]
                revision.tag = tag
                revision.type = args['type']
                if args['type'] == 'F':
                    revision.parent = None
                if args['type'] == '+':
                    if level_history:
                        # Previous merged deltas need to be re-merged
                        # at this point.
                        previous = level_history[-1]
                        previous.parent = revision.uuid

        # Clean out old backups
        for tag, args in self.schedule.items():
            for revision in self.history(tag=tag):
                age = now - revision.timestamp
                if age > args['keep']:
                    self.remove(revision)



schedule = {
    '': {'interval': 0.5*days,
           'keep': 3*days},
    'bi-weekly': {'interval': 3.5*days,
               'keep': 14*days,
               'type': '+'},
    'monthly': {'interval': 30*days,
                'keep': 180*days,
                'type': 'F'}}

schedule = {
    '': {'interval': 1*days,
           'keep': 8*days},
    'weekly': {'interval': 7*days,
               'keep': 5*7*days,
               'type': '+'},
    'monthly': {'interval': 30*days,
                'keep': 90*days,
                'type': 'F'}}

def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %a %H:%M:%S")


#import curses
#curses.initscr()

backup = Backup(schedule)

start_time = now
while now - start_time < 1200*days:
    now += 1*60*60
    backup.backup()
    backup.maintenance()
    time.sleep(0.01)

    print "="*80
    for r in backup.history():
        time_readable = format_timestamp(r.timestamp)
        print "{1} | {0.uuid} | {0.type} | {0.tag:10s} | {2}".format(
            r, time_readable, '->'.join([p.uuid for p in r.parents]))
