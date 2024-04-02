import asyncio
import datetime
import filecmp
import hashlib
import os
import random
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, Set

import yaml
from aiohttp import ClientError
from structlog.stdlib import BoundLogger

import backy.utils

from .backup import Backup
from .client import APIClientManager
from .ext_deps import BACKY_CMD
from .schedule import Schedule
from .utils import (
    SafeFile,
    format_datetime_local,
    generate_taskid,
    time_or_event,
)

if TYPE_CHECKING:
    from backy.daemon import BackyDaemon


class Job(object):
    name: str
    source: dict
    schedule_name: str
    status: str = ""
    next_time: Optional[datetime.datetime] = None
    next_tags: Optional[set[str]] = None
    path: Path
    backup: Backup
    logfile: Path
    last_config: Optional[dict] = None
    daemon: "BackyDaemon"
    run_immediately: asyncio.Event
    errors: int = 0
    backoff: int = 0
    taskid: str = ""
    log: BoundLogger

    _task: Optional[asyncio.Task] = None

    def __init__(self, daemon: "BackyDaemon", name: str, log: BoundLogger):
        self.daemon = daemon
        self.name = name
        self.log = log.bind(job_name=name, subsystem="job")
        self.run_immediately = asyncio.Event()
        self.path = self.daemon.base_dir / self.name
        self.logfile = self.path / "backy.log"

    def configure(self, config: dict) -> None:
        self.source = config["source"]
        self.schedule_name = config["schedule"]
        self.update_config()
        self.backup = Backup(self.path, self.log)
        self.last_config = config

    @property
    def spread(self) -> int:
        seed = int(hashlib.md5(self.name.encode("utf-8")).hexdigest(), 16)
        limit = max(x["interval"] for x in self.schedule.schedule.values())
        limit = int(limit.total_seconds())
        generator = random.Random()
        generator.seed(seed)
        return generator.randint(0, limit)

    @property
    def sla(self) -> bool:
        """Is the SLA currently held?

        The SLA being held is only reflecting the current status.

        It does not help to reflect on past situations that have failed as
        those are not indicators whether and admin needs to do something
        right now.
        """
        return not self.sla_overdue

    @property
    def sla_overdue(self) -> int:
        """Amount of time the SLA is currently overdue."""
        if not self.backup.clean_history:
            return 0
        if self.status.startswith("running"):
            return 0
        age = backy.utils.now() - self.backup.clean_history[-1].timestamp
        max_age = min(x["interval"] for x in self.schedule.schedule.values())
        if age > max_age * 1.5:
            return age.total_seconds()
        return 0

    @property
    def schedule(self) -> Schedule:
        return self.daemon.schedules[self.schedule_name]

    def update_status(self, status: str) -> None:
        self.status = status
        self.log.debug("updating-status", status=self.status)

    def update_config(self) -> None:
        """Writes config file for 'backy backup' subprocess."""

        # We do not want to create leading directories, only
        # the backup directory itself. If the base directory
        # does not exist then we likely don't have a correctly
        # configured environment.
        self.path.mkdir(exist_ok=True)
        config = self.path / "config"
        with SafeFile(config, encoding="utf-8") as f:
            f.open_new("wb")
            yaml.safe_dump(
                {"source": self.source, "schedule": self.schedule.config}, f
            )
            if config.exists() and filecmp.cmp(config, f.name):
                raise ValueError("not changed")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "source": self.source,
            "schedule": self.schedule.to_dict(),
        }

    async def _wait_for_deadline(self) -> Optional[Literal[True]]:
        assert self.next_time
        self.update_status("waiting for deadline")
        trigger = await time_or_event(self.next_time, self.run_immediately)
        self.run_immediately.clear()
        self.log.info("woken", trigger=trigger)
        return trigger

    async def _wait_for_leader(self, next_time: datetime.datetime) -> bool:
        api = None
        try:
            api = APIClientManager(self.daemon.peers, self.taskid, self.log)
            statuses = await asyncio.gather(
                *[api[server].fetch_status(f"^{self.name}$") for server in api],
                return_exceptions=True,
            )
            leader = None
            leader_revs = len(self.backup.get_history(clean=True, local=True))
            leader_status: "BackyDaemon.StatusDict"
            self.log.info("local-revs", local_revs=leader_revs)
            for server, status in zip(api, statuses):
                log = self.log.bind(server=server)
                if isinstance(status, BaseException):
                    log.info(
                        "server-unavailable", exc_info=status, exc_style="short"
                    )
                    continue
                num_remote_revs = status[0]["local_revs"]
                log.info("duplicate-job", remote_revs=num_remote_revs)
                if num_remote_revs > leader_revs:
                    leader_revs = num_remote_revs
                    leader = server
                    leader_status = status[0]

            log = self.log.bind(leader=leader)
            log.info("leader-found", leader_revs=leader_revs)
            if not leader:
                return False

            self.update_status(f"monitoring ({leader})")
            res = leader_status
            while True:
                if (
                    res["last_time"]
                    and (next_time - res["last_time"]).total_seconds() < 5 * 60
                ):
                    # there was a backup in the last 5min
                    log.info("leader-finished")
                    return True
                if not res["status"]:
                    log.info("leader-stopped")
                    return False
                if res["next_time"] and (
                    (res["next_time"] - next_time).total_seconds() > 5 * 60
                ):
                    # not currently running or scheduled in the next 5min
                    log.info("leader-not-scheduled")
                    return False

                if await backy.utils.delay_or_event(300, self.run_immediately):
                    self.run_immediately.clear()
                    log.info("run-immediately-triggered")
                    return False
                try:
                    res = (await api[leader].fetch_status(f"^{self.name}$"))[0]
                except ClientError:
                    log.warning("leader-failed", exc_style="short")
                    return False
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("_wait_for_leader-failed")
            return False
        finally:
            if api:
                await api.close()

    async def run_forever(self) -> None:
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
            self.taskid = generate_taskid()
            self.log = self.log.bind(job_name=self.name, sub_taskid=self.taskid)

            self.backup = Backup(self.path, self.log)

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
            run_immediately = await self._wait_for_deadline()

            # The UI shouldn't show a next any longer now that we have already
            # triggered.
            self.next_time = None
            self.next_tags = None

            try:
                self.update_status("checking neighbours")
                if not run_immediately and await self._wait_for_leader(
                    next_time
                ):
                    await self.pull_metadata()
                    await self.run_callback()
                else:
                    speed = "slow"
                    if (
                        self.backup.clean_history
                        and self.backup.clean_history[-1].stats["duration"]
                        < 600
                    ):
                        speed = "fast"
                    self.update_status(f"waiting for worker slot ({speed})")

                    async with self.daemon.backup_semaphores[speed]:
                        self.update_status(f"running ({speed})")

                        await self.run_backup(next_tags)
                        await self.pull_metadata()
                        await self.run_expiry()
                        await self.push_metadata()
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

    async def pull_metadata(self) -> None:
        self.log.info("pull-metadata-started")
        proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-t",
            self.taskid,
            "-b",
            self.path,
            "-l",
            self.logfile,
            "pull",
            "-c",
            self.daemon.config_file,
            close_fds=True,
            start_new_session=True,  # Avoid signal propagation like Ctrl-C
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            return_code = await proc.wait()
            self.log.info(
                "pull-metadata-finished",
                return_code=return_code,
                subprocess_pid=proc.pid,
            )
        except asyncio.CancelledError:
            self.log.warning("pull-metadata-cancelled")
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise

    async def push_metadata(self) -> None:
        self.log.info("push-metadata-started")
        proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-t",
            self.taskid,
            "-b",
            self.path,
            "-l",
            self.logfile,
            "push",
            "-c",
            self.daemon.config_file,
            close_fds=True,
            start_new_session=True,  # Avoid signal propagation like Ctrl-C
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            return_code = await proc.wait()
            self.log.info(
                "push-metadata-finished",
                return_code=return_code,
                subprocess_pid=proc.pid,
            )
        except asyncio.CancelledError:
            self.log.warning("push-metadata-cancelled")
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise

    async def run_backup(self, tags: Set[str]) -> None:
        self.log.info("backup-started", tags=", ".join(tags))
        proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-t",
            self.taskid,
            "-b",
            str(self.path),
            "-l",
            str(self.logfile),
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

    async def run_expiry(self) -> None:
        self.log.info("expiry-started")
        proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-t",
            self.taskid,
            "-b",
            self.path,
            "-l",
            self.logfile,
            "expire",
            close_fds=True,
            start_new_session=True,  # Avoid signal propagation like Ctrl-C
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            return_code = await proc.wait()
            self.log.info(
                "expiry-finished",
                return_code=return_code,
                subprocess_pid=proc.pid,
            )
            if return_code:
                raise RuntimeError(
                    f"Expiry failed with return code {return_code}"
                )
        except asyncio.CancelledError:
            self.log.warning("expiry-cancelled")
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise

    async def run_purge(self) -> None:
        self.log.info("purge-started")
        proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-t",
            self.taskid,
            "-b",
            str(self.path),
            "-l",
            str(self.logfile),
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

    async def run_callback(self) -> None:
        if not self.daemon.backup_completed_callback:
            self.log.debug("callback-not-configured")
            return

        self.log.info("callback-started")
        read, write = os.pipe()
        backy_proc = await asyncio.create_subprocess_exec(
            BACKY_CMD,
            "-b",
            str(self.path),
            "-l",
            str(self.logfile),
            "status",
            "--yaml",
            stdin=subprocess.DEVNULL,
            stdout=write,
            stderr=subprocess.DEVNULL,
        )
        os.close(write)
        callback_proc = await asyncio.create_subprocess_exec(
            str(self.daemon.backup_completed_callback),
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

    def start(self) -> None:
        assert self._task is None
        assert self.daemon.loop
        self._task = self.daemon.loop.create_task(
            self.run_forever(), name=f"backup-loop-{self.name}"
        )

    def stop(self) -> None:
        # XXX make shutdown graceful and let a previous run finish ...
        # schedule a reload after that.
        if self._task:
            self.log.info("stop")
            self._task.cancel()
            self._task = None
            self.update_status("")
