from .archive import Archive
from .ext_deps import BACKY_CMD
from .utils import SafeFile
import asyncio
import backy.utils
import filecmp
import hashlib
import logging
import os
import os.path as p
import random
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
        self.returncode = None

    @property
    def name(self):
        return self.job.name

    # Maybe this should be run in an executor instead?
    @asyncio.coroutine
    def backup(self, future):
        future.add_done_callback(lambda f: self.finished.set())

        logger.info('%s: running backup [%s] due %s',
                    self.name, ', '.join(self.tags),
                    self.ideal_start.isoformat())

        # XXX this isn't true async
        self.job.update_config()

        # Run backy command
        logfile = p.join(self.job.path, 'backy.log')
        cmd = [BACKY_CMD, '-b', self.job.path, '-l', logfile, 'backup',
               ','.join(self.tags)]
        # capture stdout and stderr from shellouts into logfile as well
        with open(logfile, 'a', encoding='utf-8', buffering=1) as log:
            process = yield from asyncio.create_subprocess_exec(
                *cmd, stdout=log, stderr=log)
            yield from process.communicate()
        self.returncode = process.returncode

        # Expire backups
        # TODO: this isn't true async, but it works for now.
        self.job.expire()
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
        logger.debug("starting to work on queue")
        while True:
            # Ok, lets get a worker slot and a task
            yield from self.workers.acquire()
            logger.debug('Got worker, %d idle', self.workers._value + 1)
            task = yield from self.get()
            logger.debug('Got task "%s"', task.name)
            task_future = asyncio.Future()
            self.loop.create_task(task.backup(task_future))
            task_future.add_done_callback(self.finish_task)

    def finish_task(self, future):
        task = future.result()
        if task.returncode > 0:  # pragma: no cover
            logger.error('%s: exit status %s', task.name, task.returncode)
        else:
            logger.debug('%s: success', task.name)
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
        self.path = p.join(self.daemon.base_dir, self.name)
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
        if not self.archive.clean_history:
            return True
        age = backy.utils.now() - self.archive.clean_history[-1].timestamp
        max_age = min(x['interval'] for x in self.schedule.schedule.values())
        if age > max_age * 1.5:
            return False
        return True

    @property
    def schedule(self):
        return self.daemon.schedules[self.schedule_name]

    def update_status(self, status):
        self.status = status
        logger.info('{}: status {}'.format(self.name, self.status))

    def update_config(self):
        """Writes config file for 'backy backup' subprocess."""
        if not p.exists(self.path):
            # We do not want to create leading directories, only
            # the backup directory itself. If the base directory
            # does not exist then we likely don't have a correctly
            # configured environment.
            os.mkdir(self.path)
        config = p.join(self.path, 'config')
        with SafeFile(config, encoding='utf-8') as f:
            f.open_new('wb')
            yaml.safe_dump(self.source, f)
            if p.exists(config) and filecmp.cmp(config, f.name):
                raise ValueError('not changed')

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
            self.update_status("active")
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
