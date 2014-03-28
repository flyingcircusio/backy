import time


class Stats(object):
    # Singleton
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(Stats, cls).__new__(
                cls, *args, **kwargs)
        return cls.__instance

    time_started = time.time()
    bytes_through = 0

    def t(self, length):
        self.bytes_through += length

    def __str__(self):
        mb = self.bytes_through/1024/1024
        s = time.time()-self.time_started
        return "Throughput: %4.1fMB/s (%.1fMB in %.1fs)" % (mb/s, mb, s)
