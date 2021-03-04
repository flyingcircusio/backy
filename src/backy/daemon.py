import asyncio
import collections
import fcntl
import logging
import os
import os.path as p
import re
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pkg_resources
import prettytable
import telnetlib3
import yaml

from .schedule import Schedule
from .scheduler import Job
from .utils import SafeFile, format_timestamp

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
            logger.error('Could not load configuration. `%s` does not exist.',
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
        self.status_interval = int(
            g.get('status-interval', self.status_interval))
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

        self.async_tasks.add(asyncio.ensure_future(self.save_status_file()))

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
            logger.info('starting telnet server on %s:%i', addr,
                        self.telnet_port)
            server = telnetlib3.create_server(
                host=addr, port=self.telnet_port, shell=telnet_server_shell)
            self.async_tasks.add(asyncio.ensure_future(server))

    def terminate(self):
        logger.info('Terminating all tasks')
        self.taskpool.shutdown()
        self.running = False
        logger.info('Cancelling all tasks')
        for t in self.async_tasks:
            t.cancel()
        logger.info('Stopping loop')
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
            result.append(
                dict(
                    job=job.name,
                    sla='OK' if job.sla else 'TOO OLD',
                    sla_overdue=job.sla_overdue,
                    status=job.status,
                    last_time=(format_timestamp(last.timestamp)
                               if last else '-'),
                    last_tags=(','.join(job.schedule.sorted_tags(last.tags))
                               if last else '-'),
                    last_duration=(
                        str(round(last.stats.get('duration', 0), 1)) +
                        ' s' if last else '-'),
                    next_time=(format_timestamp(job.task.ideal_start)
                               if job.task else '-'),
                    next_tags=(','.join(
                        job.schedule.sorted_tags(job.task.tags))
                               if job.task else '-')))
        return result

    def _write_status_file(self):
        status = self.status()
        with SafeFile(self.status_file, sync=False) as tmp:
            tmp.protected_mode = 0o644
            tmp.open_new('w')
            yaml.safe_dump(status, tmp.f)
        for job in status:
            if not job['sla_overdue']:
                continue
            logger.warning(
                f'SLA violation: {job["job"]} - {job["sla_overdue"]}s overdue')

    async def save_status_file(self):
        while self.running:
            try:
                self._write_status_file()
            except Exception as e:  # pragma: no cover
                logger.exception(e)
            await asyncio.sleep(self.status_interval)

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
                len(failed_jobs),
                '\n'.join('{} (last time: {}, overdue: {})'.format(
                    f['job'], f['last_time'], f['sla_overdue'])
                          for f in failed_jobs)))
            logging.debug('failed jobs: %r', failed_jobs)
            sys.exit(2)

        print("OK: {} jobs within SLA".format(len(status)))
        sys.exit(0)


async def telnet_server_shell(reader, writer):
    """
    A default telnet shell, appropriate for use with telnetlib3.create_server.
    This shell provides a very simple REPL, allowing introspection and state
    toggling of the connected client session.
    This function is a :func:`~asyncio.coroutine`.
    """
    from telnetlib3.server_shell import CR, LF, NUL, readline, get_slcdata

    writer.write('backy {}'.format(pkg_resources.require("backy")[0].version) +
                 CR + LF)
    writer.write("Ready." + CR + LF)

    linereader = readline(reader, writer)
    linereader.send(None)

    shell = SchedulerShell(writer=writer)

    command = None
    while True:
        if command:
            writer.write(CR + LF)
        writer.write('backy> ')
        command = None
        while command is None:
            # TODO: use reader.readline()
            inp = await reader.read(1)
            if not inp:
                return
            command = linereader.send(inp)
        writer.write(CR + LF)
        if command == 'quit':
            writer.write('Goodbye.' + CR + LF)
            break
        elif command == 'help':
            writer.write('jobs [filter], status, run <job>, runall')
        elif command.startswith('jobs'):
            if ' ' in command:
                _, filter_re = command.split(' ', maxsplit=1)
            else:
                filter_re = None
            shell.jobs(filter_re)
        elif command == 'status':
            shell.status()
        elif command.startswith('runall'):
            shell.runall()
        elif command.startswith('run'):
            if ' ' in command:
                _, job = command.split(' ', maxsplit=1)
            else:
                job = None
            shell.run(job)
        elif command:
            writer.write('no such command.')
    writer.close()


class SchedulerShell(object):

    writer = None

    def __init__(self, writer):
        self.writer = writer

    def jobs(self, filter_re=None):
        """List status of all known jobs. Optionally filter by regex."""
        filter_re = re.compile(filter_re) if filter_re else None
        t = prettytable.PrettyTable([
            'Job', 'SLA', 'SLA overdue', 'Status', 'Last Backup (UTC)',
            'Last Tags', 'Last Dur', 'Next Backup (UTC)', 'Next Tags'])
        t.align = 'l'
        t.align['Last Dur'] = 'r'
        t.sortby = 'Job'

        jobs = daemon.status(filter_re)
        for job in jobs:
            t.add_row([
                job['job'], job['sla'], job['sla_overdue'], job['status'],
                job['last_time'].replace(' UTC', ''), job['last_tags'],
                job['last_duration'], job['next_time'].replace(' UTC', ''),
                job['next_tags']])

        self.writer.write(t.get_string().replace('\n', '\r\n') + '\r\n')
        self.writer.write('{} jobs shown'.format(len(jobs)))

    def status(self):
        """Show job status overview"""
        t = prettytable.PrettyTable(['Status', '#'])
        state_summary = {}
        for job in daemon.jobs.values():
            state_summary.setdefault(job.status, 0)
            state_summary[job.status] += 1

        for state in sorted(state_summary):
            t.add_row([state, state_summary[state]])
        self.writer.write(t.get_string().replace('\n', '\r\n'))

    def run(self, job):
        """Show job status overview"""
        try:
            job = daemon.jobs[job]
        except KeyError:
            self.writer.write('Unknown job {}'.format(job))
            return
        if not hasattr(job, 'task'):
            self.writer.write('Task not ready. Try again later.')
            return
        job.task.run_immediately.set()
        self.writer.write('Triggered immediate run for {}'.format(job.name))

    def runall(self):
        """Show job status overview"""
        for job in daemon.jobs.values():
            if not hasattr(job, 'task'):
                self.writer.write('{} not ready. Try again later.'.format(
                    job.name))
                continue
            job.task.run_immediately.set()
            self.writer.write('Triggered immediate run for {}'.format(
                job.name))


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
