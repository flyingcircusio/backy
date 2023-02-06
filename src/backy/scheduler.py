import asyncio
import filecmp
import hashlib
import logging
import os
import os.path as p
import random
import subprocess
from datetime import timedelta

import yaml

import backy.utils

from .backup import Backup
from .ext_deps import BACKY_CMD
from .utils import SafeFile, format_datetime_local, time_or_event

logger = logging.getLogger(__name__)


class Job(object):
    name = None
    source = None
    schedule_name = None
    status = ""
    next_time = None
    next_tags = None
    path = None
    backup = None

    _task = None

    def __init__(self, daemon, name):
        self.last_config = None
        self.daemon = daemon
        self.name = name
        self.handle = None
        self.run_immediately = asyncio.Event()

    def configure(self, config):
        self.source = config["source"]
        self.schedule_name = config["schedule"]
        self.path = p.join(self.daemon.base_dir, self.name)
        self.logfile = p.join(self.path, "backy.log")
        self.update_config()
        self.backup = Backup(self.path)
        self.last_config = config

    @property
    def spread(self):
        seed = int(hashlib.md5(self.name.encode("utf-8")).hexdigest(), 16)
        limit = max(x["interval"] for x in self.schedule.schedule.values())
        limit = int(limit.total_seconds())
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
        return not (self.sla_overdue)

    @property
    def sla_overdue(self):
        """Amount of time the SLA is currently overdue."""
        if not self.backup.clean_history:
            return 0
        if self.status == "running":
            return 0
        age = backy.utils.now() - self.backup.clean_history[-1].timestamp
        max_age = min(x["interval"] for x in self.schedule.schedule.values())
        if age > max_age * 1.5:
            return age.total_seconds()
        return 0

    @property
    def schedule(self):
        return self.daemon.schedules[self.schedule_name]

    def update_status(self, status):
        self.status = status
        logger.info("{}: status {}".format(self.name, self.status))

    def update_config(self):
        """Writes config file for 'backy backup' subprocess."""
        if not p.exists(self.path):
            # We do not want to create leading directories, only
            # the backup directory itself. If the base directory
            # does not exist then we likely don't have a correctly
            # configured environment.
            os.mkdir(self.path)
        config = p.join(self.path, "config")
        with SafeFile(config, encoding="utf-8") as f:
            f.open_new("wb")
            yaml.safe_dump(
                {"source": self.source, "schedule": self.schedule.config}, f
            )
            if p.exists(config) and filecmp.cmp(config, f.name):
                raise ValueError("not changed")

    async def _wait_for_deadline(self):
        self.update_status("waiting for deadline")
        trigger = await time_or_event(self.next_time, self.run_immediately)
        self.run_immediately.clear()
        logger.info(f"{self.name}: got woken by {trigger}")
        return trigger

    async def run_forever(self):
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
        self.errors = 0
        self.backoff = 0
        logger.info(f"{self.name}: started backup loop")
        while True:
            self.backup.scan()

            next_time, next_tags = self.schedule.next(
                backy.utils.now(), self.spread, self.backup
            )

            if self.errors:
                # We're retrying - do not pay attention to the schedule but
                # we know that we have to make a backup if possible and only
                # pause for the backoff period.
                # We do, however, use the current tags just in case that we're
                # not able to make backups for a longer period.
                # This way we also use the queuing mechanism correctly so that
                # one can still manually trigger a run if one wishes to.
                next_time = backy.utils.now() + timedelta(seconds=self.backoff)

            self.next_time = next_time
            self.next_tags = next_tags
            nt = format_datetime_local(self.next_time)[0]
            logger.info(f"{self.name}: {nt}, {next_tags}")
            await self._wait_for_deadline()

            # The UI shouldn't show a next any longer now that we have already
            # triggered.
            self.next_time = None
            self.next_tags = None

            try:
                speed = "slow"
                if (
                    self.backup.clean_history
                    and self.backup.clean_history[-1].stats["duration"] < 600
                ):
                    speed = "fast"
                self.update_status(f"waiting for worker slot ({speed})")

                async with self.daemon.backup_semaphores[speed]:
                    self.update_status(f"running ({speed})")

                    self.update_config()
                    await self.run_backup(next_tags)
                    await self.run_expiry()
                    await self.run_purge()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(e)

                self.update_status("failed")
                # Something went wrong. Use bounded expontial backoff to avoid
                # hogging the workers. We sometimes can't make backups because
                # of external reasons (i.e. the VM is shut down and properly
                # making a snapshot fails
                self.errors += 1
                # Our retry series (in minutes). We converge on waiting
                # 6 hours maximum for retries.
                # 2, 4, 8, 16, 32, 65, ..., 6*60, 6*60, ...
                self.backoff = min([2**self.errors, 6 * 60]) * 60
                logger.warning(
                    "{}: retrying in {} seconds".format(self.name, self.backoff)
                )
            else:
                self.errors = 0
                self.backoff = 0
                logger.info(f"{self.name}: finished")
                self.update_status("finished")

    async def run_backup(self, tags):
        logger.info(f'{self.name}: running backup [{", ".join(tags)}]')
        # A hacky way to feed the stdout (i.e. export progress) reasonably
        # formatted ino the logfile.
        logfile = p.join(self.path, "backy.log")
        with open(logfile, "a", encoding="utf-8", buffering=1) as log:
            proc = await asyncio.create_subprocess_exec(
                BACKY_CMD,
                "-b",
                self.path,
                "-l",
                self.logfile,
                "backup",
                ",".join(tags),
                close_fds=True,
                start_new_session=True,  # Avoid signal propagation like Ctrl-C
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
            )
            try:
                rc = await proc.wait()
                logger.info(
                    f"{self.name}: finished backup with return code {rc}"
                )
                if rc:
                    raise RuntimeError(f"Backup failed with return code {rc}")
            except asyncio.CancelledError:
                logger.warning(
                    f"{self.name}: this job's backup loop was cancelled, "
                    "terminating subprocess"
                )
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
                raise

    async def run_expiry(self):
        logger.info(f"{self.name}: Expiring old revisions")
        self.schedule.expire(self.backup)

    async def run_purge(self):
        logger.info(f"{self.name}: Purging unused data")
        proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-b",
            self.path,
            "-l",
            self.logfile,
            "purge",
            # start_new_session=True,  # Avoid signal propagation like Ctrl-C.
            # close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            returncode = await proc.wait()
            logger.info(
                f"{self.name}: finished purging with return code {returncode}"
            )
        except asyncio.CancelledError:
            logger.warning(
                f"{self.name}: this job's backup loop was cancelled, "
                "terminating subprocess"
            )
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise

    def start(self):
        assert self._task is None
        self._task = self.daemon.loop.create_task(
            self.run_forever(), name=f"backup-loop-{self.name}"
        )

    def stop(self):
        # XXX make shutdown graceful and let a previous run finish ...
        # schedule a reload after that.
        if self._task:
            logger.info(f"{self.name}: shutting down")
            self._task.cancel()
            self._task = None
