import asyncio
import datetime
import filecmp
import hashlib
import os
import os.path as p
import random
import subprocess
from datetime import timedelta
from typing import Optional

import yaml
from structlog.stdlib import BoundLogger

import backy.daemon
import backy.utils

from .backup import Backup
from .ext_deps import BACKY_CMD
from .utils import SafeFile, format_datetime_local, time_or_event


class Job(object):
    name: str
    source: dict
    schedule_name: str
    status: str = ""
    next_time: Optional[datetime.datetime] = None
    next_tags: Optional[set[str]] = None
    path: Optional[str] = None
    backup: Optional[Backup] = None
    logfile: Optional[str] = None
    last_config: Optional[dict] = None
    daemon: "backy.daemon.BackyDaemon"
    run_immediately: asyncio.Event
    errors: int = 0
    backoff: int = 0
    log: BoundLogger

    _task: Optional[asyncio.Task] = None

    def __init__(self, daemon, name, log):
        self.daemon = daemon
        self.name = name
        self.log = log.bind(job_name=name, subsystem="job")
        self.run_immediately = asyncio.Event()

    def configure(self, config):
        self.source = config["source"]
        self.schedule_name = config["schedule"]
        self.path = p.join(self.daemon.base_dir, self.name)
        self.logfile = p.join(self.path, "backy.log")
        self.update_config()
        self.backup = Backup(self.path, self.log)
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
        return not self.sla_overdue

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
        self.log.debug("updating-status", status=self.status)

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
        self.log.info("woken", trigger=trigger)
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
        self.log.debug("loop-started")
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
            self.log.info(
                "waiting",
                next_time=format_datetime_local(self.next_time)[0],
                next_tags=", ".join(next_tags),
            )
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
                    await self.run_callback()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("exception")

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
                self.log.warning("backoff", backoff=self.backoff)
            else:
                self.errors = 0
                self.backoff = 0
                self.update_status("finished")

    async def run_backup(self, tags):
        self.log.info("backup-started", tags=", ".join(tags))
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
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            return_code = await proc.wait()
            self.log.info(
                "backup-finished",
                return_code=return_code,
                subprocess_pid=proc.pid,
            )
            if return_code:
                raise RuntimeError(
                    f"Backup failed with return code {return_code}"
                )
        except asyncio.CancelledError:
            self.log.warning("backup-cancelled")
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise

    async def run_expiry(self):
        self.log.info("expiring-revs")
        self.schedule.expire(self.backup)

    async def run_purge(self):
        self.log.info("purge-started")
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
            return_code = await proc.wait()
            self.log.info(
                "purge-finished",
                return_code=return_code,
                subprocess_pid=proc.pid,
            )
        except asyncio.CancelledError:
            self.log.warning("purge-cancelled", subprocess_pid=proc.pid)
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise

    async def run_callback(self):
        if not self.daemon.backup_completed_callback:
            self.log.debug("callback-not-configured")
            return

        self.log.info("callback-started")
        read, write = os.pipe()
        backy_proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-b",
            self.path,
            "-l",
            self.logfile,
            "status",
            "--yaml",
            stdin=subprocess.DEVNULL,
            stdout=write,
            stderr=subprocess.DEVNULL,
        )
        os.close(write)
        callback_proc = await asyncio.create_subprocess_exec(
            self.daemon.backup_completed_callback,
            self.name,
            stdin=read,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        os.close(read)
        try:
            stdout, stderr = await callback_proc.communicate()
            return_code1 = await backy_proc.wait()
            self.log.info(
                "callback-finished",
                return_code1=return_code1,
                return_code2=callback_proc.returncode,
                subprocess_pid1=backy_proc.pid,
                subprocess_pid2=callback_proc.pid,
                stdout=stdout.decode() if stdout else None,
                stderr=stderr.decode() if stderr else None,
            )
        except asyncio.CancelledError:
            self.log.warning(
                "callback-cancelled",
                subprocess_pid1=backy_proc.pid,
                subprocess_pid2=callback_proc.pid,
            )
            try:
                backy_proc.terminate()
            except ProcessLookupError:
                pass
            try:
                callback_proc.terminate()
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
            self.log.info("stop")
            self._task.cancel()
            self._task = None
