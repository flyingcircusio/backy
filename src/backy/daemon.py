from .schedule import Schedule
from .scheduler import Job
from .utils import format_timestamp, SafeFile
from concurrent.futures import ThreadPoolExecutor
import asyncio
import collections
import fcntl
import logging
import os
import os.path as p
import pkg_resources
import prettytable
import re
import signal
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
    status_interval = 30
    telnet_addrs = '::1, 127.0.0.1'
    telnet_port = 6023

    loop = None
    taskpool = None
    running = False
    async_tasks = None

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
        self.base_dir = g.get('base-dir', self.base_dir)
        if not self.status_file:
            self.status_file = p.join(self.base_dir, 'status')
        self.status_file = g.get('status-file', self.status_file)
        self.status_interval = int(g.get('status-interval',
                                         self.status_interval))
        self.telnet_addrs = g.get('telnet-addrs', self.telnet_addrs)
        self.telnet_port = int(g.get('telnet-port', self.telnet_port))

        new = {}
        for name, config in self.config['schedules'].items():
            if name in self.schedules:
                new[name] = self.schedules[name]
            else:
                new[name] = Schedule()
            new[name].configure(config)
        self.schedules = new

        logger.debug('status interval: %s', self.status_interval)
        logger.debug('status location: %s', self.status_file)
        logger.debug('worker limit: %s', self.worker_limit)
        logger.debug('backup location: %s', self.base_dir)
        logger.debug('available schedules: %s', ', '.join(self.schedules))

    def _setup_jobs(self):
        # To be called after _read_config().
        self.jobs = {}
        for name, config in self.config['jobs'].items():
            self.jobs[name] = Job(self, name)
            self.jobs[name].configure(config)
        logger.debug('configured jobs: %s', ', '.join(self.jobs.keys()))

    def lock(self):
        """Ensures that only a single daemon instance is active."""
        lockfile = p.join(self.base_dir, '.lock')
        self._lock = open(lockfile, 'a+b')
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except EnvironmentError as e:
            logger.critical('failed to acquire lock %s: %s', lockfile, e)
            sys.exit(69)

    def _prepare(self, loop):  # pragma: no cover
        """Starts the scheduler daemon."""
        self._read_config()
        self._setup_jobs()
        # Ensure single daemon instance.
        self.lock()

        self.loop = loop
        self.taskpool = ThreadPoolExecutor(self.worker_limit)
        self.loop.set_default_executor(self.taskpool)

        self.running = True
        self.async_tasks = set()

    def start(self, loop):
        self._prepare(loop)

        # semi-daemonize
        os.chdir(self.base_dir)
        with open('/dev/null', 'r+b') as null:
            os.dup2(null.fileno(), 0)

        for job in self.jobs.values():
            job.start()

        self.async_tasks.add(asyncio.async(self.save_status_file()))

        def sighandler(signum, _traceback):
            logger.info('Received signal %s', signum)
            self.terminate()

        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGQUIT,
                    signal.SIGTERM):
            signal.signal(sig, sighandler)

    def telnet_server(self):
        """Starts to listen on all configured telnet addresses."""
        assert self.loop, 'cannot start telnet server without event loop'
        for addr in (a.strip() for a in self.telnet_addrs.split(',')):
            logger.info('starting telnet server on %s:%i',
                        addr, self.telnet_port)
            self.async_tasks.add(asyncio.async(self.loop.create_server(
                lambda: telnetlib3.TelnetServer(shell=SchedulerShell),
                addr, self.telnet_port)))

    def terminate(self):
        logger.info('Terminating all tasks')
        self.taskpool.shutdown()
        self.running = False
        for t in self.async_tasks:
            t.cancel()
        self.loop.stop()

    def status(self, filter_re=None):
        """Collects status information for all jobs."""
        result = []
        for job in self.jobs.values():
            if filter_re and not filter_re.search(job.name):
                continue
            job.backup.scan()
            if job.backup.clean_history:
                last = job.backup.clean_history[-1]
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
        with SafeFile(self.status_file, sync=False) as tmp:
            tmp.protected_mode = 0o644
            tmp.open_new('w')
            yaml.safe_dump(self.status(), tmp.f)

    @asyncio.coroutine
    def save_status_file(self):
        while self.running:
            try:
                self._write_status_file()
            except Exception as e:  # pragma: no cover
                logger.exception(e)
            yield from asyncio.sleep(self.status_interval)

    def check(self):
        self._read_config()

        if not p.exists(self.status_file):
            print('UNKNOWN: No status file found at {}'.format(
                self.status_file))
            sys.exit(3)

        # The output should be relatively new. Let's say 5 min max.
        s = os.stat(self.status_file)
        if time.time() - s.st_mtime > 5 * 60:
            print('CRITICAL: Status file is older than 5 minutes')
            sys.exit(2)

        failed_jobs = []

        with open(self.status_file, encoding='utf-8') as f:
            status = yaml.safe_load(f)

        for job in status:
            if job['sla'] != 'OK':
                failed_jobs.append(job)

        if failed_jobs:
            print('CRITICAL: {} jobs not within SLA\n{}'.format(
                len(failed_jobs), '\n'.join(
                    '{} (last time: {})'.format(f['job'], f['last_time'])
                    for f in failed_jobs)))
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
            'Job', 'SLA', 'Status', 'Last Backup (UTC)', 'Last Tags',
            'Last Dur', 'Next Backup (UTC)', 'Next Tags'])
        t.align = 'l'
        t.align['Last Dur'] = 'r'
        t.sortby = 'Job'

        jobs = daemon.status(filter_re)
        for job in jobs:
            t.add_row([job['job'],
                       job['sla'],
                       job['status'],
                       job['last_time'].replace(' UTC', ''),
                       job['last_tags'],
                       job['last_duration'],
                       job['next_time'].replace(' UTC', ''),
                       job['next_tags']])

        self.stream.write(t.get_string().replace('\n', '\r\n') + '\r\n')
        self.stream.write('{} jobs shown'.format(len(jobs)))
        return 0

    def cmdset_status(self, *_args):
        """Show job status overview"""
        t = prettytable.PrettyTable(['Status', '#'])
        state_summary = {}
        for job in daemon.jobs.values():
            state_summary.setdefault(job.status, 0)
            state_summary[job.status] += 1

        for state in sorted(state_summary):
            t.add_row([state, state_summary[state]])
        self.stream.write(t.get_string().replace('\n', '\r\n'))
        return 0

    def cmdset_run(self, job, *extra_args):
        """Show job status overview"""
        job = daemon.jobs[job]
        if not hasattr(job, 'task'):
            self.stream.write('Task not ready. Try again later.')
            return 1
        job.task.run_immediately.set()
        self.stream.write('Triggered immediate run for {}'.format(job.name))
        return 0

    def cmdset_runall(self, *extra_args):
        """Show job status overview"""
        for job in daemon.jobs.values():
            if not hasattr(job, 'task'):
                self.stream.write(
                    '{} not ready. Try again later.'.format(job.name))
                continue
            job.task.run_immediately.set()
            self.stream.write(
                'Triggered immediate run for {}'.format(job.name))
        return 0

    autocomplete_cmdset = collections.OrderedDict([
        ('jobs', None),
        ('status', None),
        ('run', None),
        ('quit', None),
    ])

    autocomplete_cmdset.update(collections.OrderedDict([
        ('help', autocomplete_cmdset)]))


def check(config_file):  # pragma: no cover
    daemon = BackyDaemon(config_file)
    daemon.check()


def main(config_file):  # pragma: no cover
    global daemon

    loop = asyncio.get_event_loop()
    daemon = BackyDaemon(config_file)
    daemon.start(loop)
    daemon.telnet_server()

    try:
        # Blocking call
        loop.run_forever()
    except KeyboardInterrupt:
        logger.warning('Caught keyboard interrupt')
        daemon.terminate()
    finally:
        loop.stop()
        loop.close()
