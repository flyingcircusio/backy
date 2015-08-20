# Implement spread (number of minutes compared to midnight when
# a specific job has midnight.)

import random


now = random.randint(1400, 1403)


class Schedule(object):

    tags = {'h': 3,
            'd': 12,
            'w': 12 * 3,
            'm': 12 * 6}

    next_time = None
    next_tags = ()

    def __init__(self):
        self.spread = 10
        self.quant = min(self.tags.values())

    def schedule(self, relative):
        next_times = {}
        for tag, interval in self.tags.items():
            t = next_times.setdefault(
                relative + interval - ((self.spread + relative) % interval), [])
            t.append(tag)
        self.next_time = list(sorted(next_times))[0]
        self.next_tags = next_times[self.next_time]

    def tick(self):
        if self.next_time is None:
            self.schedule(now)

        if now >= self.next_time:
            print("{}: {}".format(now, self.next_tags))
        else:
            return

        self.schedule(self.next_time)


s = Schedule()
ticks = 0

while ticks <= 200:
    s.tick()
    ticks += 1
    now += 1
