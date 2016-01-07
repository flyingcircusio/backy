from .schedule import Schedule
from .scheduler import Job, TaskPool
from .utils import format_timestamp, SafeFile
import asyncio
import collections
import fcntl
import logging
import os
import os.path as p
import pkg_resources
import prettytable
import re
import sys
import telnetlib3
import time
import yaml

logger = logging.getLogger(__name__)
daemon = None


class BackyDaemon(object):

    # config defaults, will be overriden from config file
    worker_limit = 1
    base_dir = '/srv/backy'
    status_file = None

    def __init__(self, config_file):
        self.config_file = config_file
        self.config = None
        self.schedules = {}
        self.jobs = {}
        self._lock = None

    def _read_config(self):
        if not p.exists(self.config_file):
            logger.error(
                'Could not load configuration. `%s` does not exist.',
                self.config_file)
            raise SystemExit(1)
        with open(self.config_file, encoding='utf-8') as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            self.config = yaml.safe_load(f)

        g = self.config.get('global', {})
        self.worker_limit = int(g.get('worker-limit', self.worker_limit))
        logger.info("Worker limit: %s", self.worker_limit)

        self.base_dir = g.get('base-dir', self.base_dir)
        logger.info("Backup location: %s", self.base_dir)

        if not self.status_file:
            self.status_file = p.join(self.base_dir, 'status')
        self.status_file = g.get('status-file', self.status_file)
        logger.info("Status location: %s", self.status_file)

        new = {}
        for name, config in self.config['schedules'].items():
            if name in self.schedules:
                new[name] = self.schedules[name]
            else:
                new[name] = Schedule()
            new[name].configure(config)
        self.schedules = new
        logger.info("Available schedules: %s", ', '.join(self.schedules))

        self.jobs = {}
        for name, config in self.config['jobs'].items():
            self.jobs[name] = Job(self, name)
            self.jobs[name].configure(config)

    def start(self, loop):
        """Starts the scheduler daemon."""
        # Ensure single daemon instance.
        self._read_config()
        self._lock = open(self.status_file, 'a+b')
        fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # semi-daemonize
        os.chdir(self.base_dir)
        with open('/dev/null', 'r+b') as null:
            os.dup2(null.fileno(), 0)

        self.loop = loop
        self.taskpool = TaskPool(loop, self.worker_limit)
        asyncio.async(self.taskpool.run())
        asyncio.async(self.save_status_file())

        # Start jobs
        for job in self.jobs.values():
            job.start()

    def status(self, filter_re=None):
        result = []
        for job in self.jobs.values():
            if filter_re and not filter_re.search(job.name):
                continue
            job.archive.scan()
            if job.archive.clean_history:
                last = job.archive.clean_history[-1]
            else:
                last = None
            result.append(dict(
                job=job.name,
                sla='OK' if job.sla else 'TOO OLD',
                status=job.status,
                last_time=(format_timestamp(last.timestamp)
                           if last else '-'),
                last_tags=(','.join(job.schedule.sorted_tags(last.tags))
                           if last else '-'),
                last_duration=(
                    str(round(last.stats.get('duration', 0), 1)) + ' s'
                    if last else '-'),
                next_time=(format_timestamp(job.task.ideal_start)
                           if job.task else '-'),
                next_tags=(','.join(job.schedule.sorted_tags(job.task.tags))
                           if job.task else '-')))
        return result

    def _write_status_file(self):
        with SafeFile(self.status_file) as tmp:
            tmp.protected_mode = 0o644
            tmp.open_new('w')
            yaml.safe_dump(self.status(), tmp.f)

    @asyncio.coroutine
    def save_status_file(self):
        while True:
            self._write_status_file()
            yield from asyncio.sleep(30)

    def check(self):
        # This should be transformed into the nagiosplugin output. I can't make
        # myself wrap my head around its structure, though.
        self._read_config()

        if not p.exists(self.status_file):
            print("UNKNOWN: No status file found at {}".format(
                self.status_file))
            sys.exit(3)

        # The output should be relatively new. Let's say 5 min max.
        s = os.stat(self.status_file)
        if time.time() - s.st_mtime > 5 * 60:
            print("CRITICAL: Status file is older than 5 minutes.")
            sys.exit(2)

        failed_jobs = []

        with open(self.status_file, encoding='utf-8') as f:
            status = yaml.safe_load(f)

        for job in status:
            if job['sla'] != 'OK':
                failed_jobs.append(job)

        if failed_jobs:
            print('CRITICAL: {} jobs not within SLA\n{}'.format(
                len(failed_jobs), '\n'.join(f['job'] for f in failed_jobs)))
            logging.debug('failed jobs: %r', failed_jobs)
            sys.exit(2)

        print("OK: {} jobs within SLA".format(len(status)))
        sys.exit(0)


class SchedulerShell(telnetlib3.Telsh):

    shell_name = 'backy'
    shell_ver = pkg_resources.require("backy")[0].version

    def cmdset_jobs(self, filter_re=None, *extra_args):
        """List status of all known jobs. Optionally filter by regex."""
        if len(extra_args) > 0:
            self.stream.write('Usage: jobs [REGEX]')
            return 1
        filter_re = re.compile(filter_re) if filter_re else None
        t = prettytable.PrettyTable([
            'Job', 'SLA', 'Status', 'Last Backup', 'Last Tags', 'Last Dur',
            'Next Backup', 'Next Tags'])
        t.align = 'l'
        t.align['Last Dur'] = 'r'
        t.sortby = 'Job'

        for job in daemon.status(filter_re):
            t.add_row([job['job'],
                       job['sla'],
                       job['status'],
                       job['last_time'],
                       job['last_tags'],
                       job['last_duration'],
                       job['next_time'],
                       job['next_tags']])

        self.stream.write(t.get_string().replace('\n', '\r\n'))
        return 0

    def cmdset_status(self, *_args):
        """Show worker status"""
        t = prettytable.PrettyTable(['Property', 'Status'])
        t.add_row(['Idle Workers', daemon.taskpool.workers._value + 1])
        t.sortby = 'Property'
        self.stream.write(t.get_string().replace('\n', '\r\n'))
        return 0

    autocomplete_cmdset = collections.OrderedDict([
        ('jobs', None),
        ('status', None),
        ('quit', None),
    ])

    autocomplete_cmdset.update(collections.OrderedDict([
        ('help', autocomplete_cmdset)]))


def check(config_file):
    daemon = BackyDaemon(config_file)
    daemon.check()


def main(config_file):
    global daemon

    loop = asyncio.get_event_loop()
    daemon = BackyDaemon(config_file)
    daemon.start(loop)

    logger.info('starting telnet server on localhost:6023')
    func = loop.create_server(
        lambda: telnetlib3.TelnetServer(shell=SchedulerShell),
        '127.0.0.1', 6023)
    loop.run_until_complete(func)

    # Blocking call interrupted by loop.stop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
