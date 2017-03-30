from .backup import Backup
from .ext_deps import BACKY_CMD
from .utils import SafeFile
from datetime import timedelta
import asyncio
import backy.utils
import filecmp
import hashlib
import logging
import os
import os.path as p
import random
import subprocess
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
        self.run_immediately = asyncio.Event()

    @property
    def name(self):
        return self.job.name

    # Run this in an executor!
    def backup(self):
        returncode = 1
        try:
            self.job.update_status('running')
            logger.info('%s: running backup [%s] due %s',
                        self.name, ', '.join(self.tags),
                        self.ideal_start.isoformat())

            self.job.update_config()

            # Run backy command
            logfile = p.join(self.job.path, 'backy.log')
            cmd = [BACKY_CMD, '-b', self.job.path, '-l', logfile, 'backup',
                   ','.join(self.tags)]
            # capture stdout and stderr from shellouts into logfile as well
            with open(logfile, 'a', encoding='utf-8', buffering=1) as log:
                returncode = subprocess.call(cmd, stdout=log, stderr=log)

            logger.info('%s: finished backup with return code %s',
                        self.name, returncode)

            logger.info('%s: Expiring old revisions', self.name)
            self.job.schedule.expire(self.job.backup)

            # Purge
            logger.info('%s: Purging unused data', self.name)
            cmd = [BACKY_CMD, '-b', self.job.path, '-l', logfile, 'purge']
            purge_returncode = subprocess.call(cmd)
            logger.info('%s: finished purging with return code %s',
                        self.name, purge_returncode)
        except Exception as e:
            # I'm not resetting the return code here. If we managed to make
            # a backup then we are not picky about whether expiry or purging
            # went fine.
            logger.exception(e)
        return returncode

    @asyncio.coroutine
    def wait_for_deadline(self):
        remaining_time = self.ideal_start - backy.utils.now()
        print(self.name, self.ideal_start, remaining_time)
        yield from next(asyncio.as_completed([
            asyncio.sleep(remaining_time.total_seconds()),
            self.run_immediately.wait()]))


class Job(object):

    name = None
    source = None
    schedule_name = None
    status = ''
    task = None
    path = None
    backup = None
    running = False

    _generator_handle = None

    def __init__(self, daemon, name):
        self.daemon = daemon
        self.name = name
        self.handle = None

    def configure(self, config):
        self.source = config['source']
        self.schedule_name = config['schedule']
        self.path = p.join(self.daemon.base_dir, self.name)
        self.update_config()
        self.backup = Backup(self.path)

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
        if not self.backup.clean_history:
            return True
        age = backy.utils.now() - self.backup.clean_history[-1].timestamp
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
        errors = 0
        backoff = 0
        logger.info("%s: started task generator loop", self.name)
        while True:
            next_time, next_tags = self.schedule.next(
                backy.utils.now(), self.spread, self.backup)

            if errors:
                # We're retrying - do not pay attention to the schedule but
                # we know that we have to make a backup if possible and only
                # pause for the backoff period.
                # We do, however, use the current tags just in case that we're
                # not able to make backups for a longer period.
                # This way we also use the queuing mechanism correctly so that
                # one can still manually trigger a run if one wishes to.
                next_time = (
                    backy.utils.now() + timedelta(seconds=backoff))

            self.task = task = Task(self)
            self.task.ideal_start = next_time
            self.task.tags.update(next_tags)

            self.update_status("waiting for deadline")
            yield from task.wait_for_deadline()
            logger.info("%s: got deadline trigger", self.name)
            self.update_status("queued for execution")
            returncode = yield from self.daemon.loop.run_in_executor(
                None, self.task.backup)
            if returncode:
                self.update_status("failed")
                # Something went wrong. Use truncated expontial backoff to
                # avoid hogging the workers. We sometimes can't make backups
                # because of external reasons (i.e. the VM is shut down and
                # properly making a snapshot fails
                errors += 1
                # Our retry series (in minutes). We converge on waiting
                # 6 hours maximum for retries.
                # 2, 4, 8, 16, 32, 65, ..., 6*60, 6*60
                backoff = min([2**errors, 6 * 60]) * 60
                logger.warn('{}: retrying in {} seconds'.format(
                    self.name, backoff))
            else:
                errors = 0
                backoff = None
                self.update_status("finished")

    def start(self):
        self.stop()
        self._generator_handle = asyncio.async(self.generate_tasks())

    def stop(self):
        if self._generator_handle:
            self._generator_handle.cancel()
            self._generator_handle = None
