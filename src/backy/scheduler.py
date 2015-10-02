from .backup import Archive
from .ext_deps import BACKY_CMD
from .schedule import Schedule
from prettytable import PrettyTable
import asyncio
import backy.utils
import collections
import datetime
import fcntl
import hashlib
import logging
import os
import pkg_resources
import random
import sys
import telnetlib3
import time
import yaml


logger = logging.getLogger(__name__)


class Task(object):
    """A single backup task with a specific set of tags to be executed
    at a specific time.
    """

    ideal_start = None

    def __init__(self, job):
        self.job = job
        self.tags = set()
        self.finished = asyncio.Event()

    @property
    def name(self):
        return self.job.name

    # Maybe this should be run in an executor instead?
    @asyncio.coroutine
    def backup(self, future):
        future.add_done_callback(lambda f: self.finished.set())

        logger.info('%s: running backup %s, was due at %s',
                    self.job.name, ', '.join(self.tags),
                    self.ideal_start.isoformat())

        # Update config
        # TODO: this isn't true async, but it works for now.
        backup_dir = os.path.join(
            self.job.daemon.config['global']['base-dir'],
            self.name)
        if not os.path.exists(backup_dir):
            # We do not want to create leading directories, only
            # the backup directory itself. If the base directory
            # does not exist then we likely don't have a correctly
            # configured environment.
            os.mkdir(backup_dir)
        with open(os.path.join(backup_dir, 'config'), 'w') as f:
            yaml.safe_dump(self.job.source, f)

        # Run backup command
        cmd = "{} -b {} backup {}".format(
            BACKY_CMD, backup_dir, ','.join(self.tags))
        process = yield from asyncio.create_subprocess_shell(cmd)
        yield from process.communicate()

        # Expire backups
        # TODO: this isn't true async, but it works for now.
        self.job.expire()

        logger.info("%s: finished backup", self.job.name)
        future.set_result(self)

    @asyncio.coroutine
    def wait_for_deadline(self):
        while self.ideal_start > backy.utils.now():
            remaining_time = self.ideal_start - backy.utils.now()
            yield from asyncio.sleep(remaining_time.total_seconds())

    @asyncio.coroutine
    def wait_for_finished(self):
        yield from self.finished.wait()


class TaskPool(object):
    """Processes tasks by assigning workers from a limited pool ASAP.

    Chooses the next task based on its relative priority (it's ideal
    start date).

    Any task inserted into the pool will be worked on ASAP. Tasks are
    not checked whether their deadline is due.
    """

    def __init__(self, loop, limit=2):
        self.loop = loop
        self.tasks = asyncio.PriorityQueue()
        self.workers = asyncio.BoundedSemaphore(limit)

    @asyncio.coroutine
    def put(self, task):
        """Insert task into queue for processing when due."""
        yield from self.tasks.put((task.ideal_start, task))

    @asyncio.coroutine
    def get(self):
        priority, task = yield from self.tasks.get()
        return task

    @asyncio.coroutine
    def run(self):
        logger.info("Starting to work on queue")
        while True:
            # Ok, lets get a worker slot and a task
            yield from self.workers.acquire()
            logger.debug("Got worker")
            task = yield from self.get()
            logger.debug("Got task")
            task_future = asyncio.Future()
            # Now, lets select a job
            logger.debug("Sending task to event loop")
            self.loop.create_task(task.backup(task_future))
            task_future.add_done_callback(self.finish_task)

    def finish_task(self, future):
        task = future.result()
        logger.info("{}: finished, releasing worker.".format(task.name))
        self.workers.release()


class Job(object):

    name = None
    source = None
    schedule_name = None
    status = ''
    task = None
    path = None
    archive = None

    _generator_handle = None

    def __init__(self, daemon, name):
        self.daemon = daemon
        self.name = name
        self.handle = None

    def configure(self, config):
        self.source = config['source']
        self.schedule_name = config['schedule']
        self.path = self.daemon.base_dir + '/' + self.name
        self.archive = Archive(self.path)

    @property
    def spread(self):
        seed = int(hashlib.md5(self.name.encode('utf-8')).hexdigest(), 16)
        limit = max(x['interval'] for x in self.schedule.schedule.values())
        limit = limit.total_seconds()
        generator = random.Random()
        generator.seed(seed)
        return generator.randint(0, limit)

    @property
    def sla(self):
        """Is the SLA currently held?

        The SLA being held is only reflecting the current status.

        It does not help to reflect on past situations that have failed as
        those are not indicators whether and admin needs to do something
        right now.
        """
        max_age = min(x['interval'] for x in self.schedule.schedule.values())
        self.archive.scan()
        if not self.archive.history:
            return True
        newest = self.archive.history[-1]
        age = backy.utils.now() - newest.timestamp
        if age > max_age * 1.5:
            return False
        return True

    @property
    def schedule(self):
        return self.daemon.schedules[self.schedule_name]

    def update_status(self, status):
        self.status = status
        logger.info('{}: {}'.format(self.name, self.status))

    @asyncio.coroutine
    def generate_tasks(self):
        """Generate backup tasks for this job.

        Tasks are based on the ideal next time in the future and
        previous tasks to ensure we catch up quickly if the next job in
        the future is too far away.

        This may repetetively submit a task until its time has come and
        then generate other tasks after the ideal next time has switched
        over.

        It doesn't care whether the tasks have been successfully worked
        on or not. The task pool needs to deal with that.
        """
        logger.info("%s: started task generator loop", self.name)
        while True:
            next_time, next_tags = self.schedule.next(
                backy.utils.now(), self.spread, self.archive)

            self.task = task = Task(self)
            self.task.ideal_start = next_time
            self.task.tags.update(next_tags)

            self.update_status("waiting")
            yield from task.wait_for_deadline()
            self.update_status("submitting to worker queue")
            yield from self.daemon.taskpool.put(task)
            # XXX This status is a bit of a lie
            self.update_status("running")
            yield from task.wait_for_finished()
            self.update_status("finished")

    def start(self):
        self.stop()
        self._generator_handle = asyncio.async(self.generate_tasks())

    def stop(self):
        if self._generator_handle:
            self._generator_handle.cancel()
            self._generator_handle = None

    def expire(self):
        """Remove old revisions from my archive according to the schedule."""
        return self.schedule.expire(self.archive)


class BackyDaemon(object):

    # config defaults, will be overriden from config file
    worker_limit = 1
    base_dir = '/srv/backy'
    status_file = os.path.join(base_dir, 'status')

    def __init__(self, config_file):
        self.config_file = config_file
        self.config = None
        self.schedules = {}
        self.jobs = {}

    def _read_config(self):
        if not os.path.exists(self.config_file):
            logger.error(
                'Could not load configuration. `%s` does not exist.',
                self.config_file)
            raise SystemExit(1)
        with open(self.config_file, encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        g = self.config.get('global', {})
        self.worker_limit = int(g.get('worker-limit', self.worker_limit))
        logger.info("Worker limit: %s", self.worker_limit)

        self.base_dir = g.get('base-dir', self.base_dir)
        logger.info("Backup location: %s", self.base_dir)

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
        # Ensure single daemon instance.
        f = open(self.config_file, 'r+')
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

        self.loop = loop
        self._read_config()
        self.taskpool = TaskPool(loop, self.worker_limit)
        asyncio.async(self.taskpool.run())
        asyncio.async(self.save_status_file())

        # Start jobs
        for job in self.jobs.values():
            job.start()

    def status(self):
        result = []
        for job in daemon.jobs.values():
            job.archive.scan()
            if job.archive.history:
                last = job.archive.history[-1]
            else:
                last = None
            result.append(dict(
                job=job.name,
                sla='OK' if job.sla else 'TOO OLD',
                status=job.status,
                last_time=(backy.utils.format_timestamp(last.timestamp)
                           if last else '-'),
                last_tags=(', '.join(job.schedule.sorted_tags(last.tags))
                           if last else '-'),
                last_duration=str(
                    datetime.timedelta(seconds=last.stats.get('duration', 0))
                    if last else '-'),
                next_time=(backy.utils.format_timestamp(job.task.ideal_start)
                           if job.task else '-'),
                next_tags=(', '.join(job.schedule.sorted_tags(job.task.tags))
                           if job.task else '-')))
        return result

    @asyncio.coroutine
    def save_status_file(self):
        while True:
            with backy.utils.SafeFile(self.status_file) as tmp:
                tmp.protected_mode = 0o444
                tmp.open_new('w')
                yaml.safe_dump(self.status(), tmp.f)
            yield from asyncio.sleep(30)

    def check(self):
        # This should be transformed into the nagiosplugin output. I can't make
        # myself wrap my head around its structure, though.
        self._read_config()

        if not os.path.exists(self.status_file):
            print("UNKNOWN: No status file found at {}".format(
                self.status_file))
            sys.exit(3)

        # The output should be relatively new. Let's say 5 min max.
        s = os.stat(self.status_file)
        if time.time() - s.st_mtime > 5 * 60:
            print("CRITICAL: Status file is older than 5 minutes.")
            sys.exit(2)

        failed_jobs = []

        with open(self.status_file) as f:
            status = yaml.safe_load(f)

        for job in status:
            if job['sla'] != 'OK':
                failed_jobs.append(job)

        if failed_jobs:
            print("CRITICAL: {} jobs not within SLA".format(len(failed_jobs)))
            sys.exit(2)

        print("OK: {} jobs within SLA".format(len(status)))
        sys.exit(0)


class SchedulerShell(telnetlib3.Telsh):

    shell_name = 'backy'
    shell_ver = pkg_resources.require("backy")[0].version

    def cmdset_jobs(self, *_args):
        """List status of all known jobs"""
        t = PrettyTable(["Job",
                         "SLA",
                         "Status",
                         "Last Backup",
                         "Last Tags",
                         "Last Duration",
                         "Next Backup",
                         "Next Tags"])

        for job in daemon.status():
            t.add_row([job['job'],
                       job['sla'],
                       job['status'],
                       job['last_time'],
                       job['last_tags'],
                       job['last_duration'],
                       job['next_time'],
                       job['next_tags']])

        t.sortby = "Job"
        t.align = 'l'
        self.stream.write(t.get_string().replace('\n', '\r\n'))
        return 0

    def cmdset_status(self, *_args):
        """Show worker status"""
        t = PrettyTable(["Property", "Status"])
        t.add_row(["Idle Workers", daemon.taskpool.workers._value + 1])
        t.sortby = "Property"
        self.stream.write(t.get_string().replace('\n', '\r\n'))
        return 0

    autocomplete_cmdset = collections.OrderedDict([
        ('jobs', None),
        ('status', None),
        ('quit', None),
    ])

    autocomplete_cmdset.update(collections.OrderedDict([
        ('help', autocomplete_cmdset)]))


daemon = None


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
