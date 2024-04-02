import asyncio
import datetime
import fcntl
import os
import os.path as p
import signal
import sys
import time
from pathlib import Path
from typing import IO, List, Optional, Pattern, TypedDict

import aiofiles.os as aos
import aioshutil
import yaml
from structlog.stdlib import BoundLogger

from .api import BackyAPI
from .backup import Backup
from .revision import filter_manual_tags
from .schedule import Schedule
from .scheduler import Job
from .utils import has_recent_changes, is_dir_no_symlink

daemon: "BackyDaemon"


class BackyDaemon(object):
    # config defaults, will be overriden from config file
    worker_limit: int = 1
    base_dir: Path
    backup_completed_callback: Optional[Path]
    api_addrs: List[str]
    api_port: int = 6023
    api_tokens: dict[str, str]
    api_cli_default: dict
    peers: dict[str, dict]
    config_file: Path
    config: dict
    schedules: dict[str, Schedule]
    jobs: dict[str, Job]
    dead_backups: dict[str, Backup]

    backup_semaphores: dict[str, asyncio.BoundedSemaphore]
    log: BoundLogger
    _lock: Optional[IO] = None
    reload_api: asyncio.Event

    loop: Optional[asyncio.AbstractEventLoop] = None

    def __init__(self, config_file: Path, log: BoundLogger):
        self.config_file = config_file
        self.log = log.bind(subsystem="daemon")
        self.config = {}
        self.schedules = {}
        self.backup_semaphores = {}
        self.jobs = {}
        self.dead_backups = {}
        self._lock = None
        self.reload_api = asyncio.Event()
        self.api_addrs = ["::1", "127.0.0.1"]
        self.api_tokens = {}
        self.api_cli_default = {}
        self.peers = {}

    def _read_config(self):
        if not self.config_file.exists():
            self.log.error("no-config-file", config_file=str(self.config_file))
            raise RuntimeError("Could not find config file.")
        with self.config_file.open(encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            self.config = yaml.safe_load(f)

        g = self.config.get("global", {})
        self.worker_limit = int(g.get("worker-limit", type(self).worker_limit))
        self.base_dir = Path(g.get("base-dir"))
        callback = g.get("backup-completed-callback")
        self.backup_completed_callback = Path(callback) if callback else None

        self.peers = self.config.get("peers", {})

        api = self.config.get("api", {})
        self.api_addrs = [
            a.strip() for a in api.get("addrs", "::1, 127.0.0.1").split(",")
        ]

        self.api_port = int(api.get("port", type(self).api_port))
        self.api_tokens = api.get("tokens", {})
        self.api_cli_default = api.get("cli-default", {})
        if (
            self.api_addrs
            and self.api_port
            and "url" not in self.api_cli_default
        ):
            self.api_cli_default[
                "url"
            ] = f"http://{self.api_addrs[0]}:{self.api_port}"

        new = {}
        for name, config in self.config["schedules"].items():
            if name in self.schedules:
                new[name] = self.schedules[name]
            else:
                new[name] = Schedule()
            new[name].configure(config)
        self.schedules = new

        self.log.debug(
            "read-config",
            worker_limit=self.worker_limit,
            base_dir=self.base_dir,
            schedules=", ".join(self.schedules),
            api_addrs=self.api_addrs,
            api_port=self.api_port,
        )

    def _apply_config(self):
        # Add new jobs and update existing jobs
        for name, config in self.config["jobs"].items():
            if name not in self.jobs:
                self.jobs[name] = Job(self, name, self.log)
                self.log.debug("added-job", job_name=name)
            job = self.jobs[name]
            if config != job.last_config:
                self.log.info("changed-job", job_name=name)
                job.stop()
                job.configure(config)
                job.start()

        for name, job in list(self.jobs.items()):
            if name not in self.config["jobs"]:
                job.stop()
                del self.jobs[name]
                self.log.info("deleted-job", job_name=name)

        self.dead_backups.clear()
        for b in os.scandir(self.base_dir):
            if b.name in self.jobs or not b.is_dir(follow_symlinks=False):
                continue
            try:
                self.dead_backups[b.name] = Backup(
                    self.base_dir / b.name,
                    self.log.bind(job_name=b.name),
                )
                self.log.info("found-backup", job_name=b.name)
            except Exception:
                self.log.info(
                    "invalid-backup", job_name=b.name, exc_style="short"
                )

        if (
            not self.backup_semaphores
            or self.backup_semaphores["slow"]._bound_value != self.worker_limit  # type: ignore
        ):
            # Adjusting the settings of a semaphore is not obviously simple. So
            # here is a simplified version with hopefully clear semantics:
            # when the semaphores are replaced all the currently running +
            # waiting backups will still use the old semaphores. This may
            # cause a total of 2*old_limit + 2*new_limit of jobs to be active
            # at one time until this settings.
            # We only change the semaphores when the settings actually change
            # to avoid unnecessary overlap.
            self.backup_semaphores["slow"] = asyncio.BoundedSemaphore(
                self.worker_limit
            )
            self.backup_semaphores["fast"] = asyncio.BoundedSemaphore(
                self.worker_limit
            )

    def lock(self):
        """Ensures that only a single daemon instance is active."""
        lockfile = self.base_dir.with_suffix(self.base_dir.suffix + ".lock")
        self._lock = lockfile.open("a+b")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except EnvironmentError:
            self.log.critical("lock-failed", lockfile=lockfile, exc_info=True)
            sys.exit(69)

    def start(self, loop):
        """Starts the scheduler daemon."""
        self.loop = loop

        self._read_config()

        os.chdir(self.base_dir)
        # Ensure single daemon instance.
        self.lock()

        self._apply_config()

        loop.create_task(self.purge_old_files(), name="purge-old-files")
        loop.create_task(
            self.purge_pending_backups(), name="purge-pending-backups"
        )
        loop.create_task(self.shutdown_loop(), name="shutdown-cleanup")

        def handle_signals(signum):
            self.log.info("signal-received", signum=signum)

            if signum in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
                self.terminate()
            elif signum == signal.SIGHUP:
                self.reload()

        for sig in (
            signal.SIGHUP,
            signal.SIGINT,
            signal.SIGQUIT,
            signal.SIGTERM,
        ):
            loop.add_signal_handler(sig, handle_signals, sig)

    def run_forever(self):
        assert self.loop
        self.log.info("starting-loop")
        self.loop.run_forever()
        self.loop.close()

    def reload(self):
        self.log.info("reloading")
        try:
            self._read_config()
            self._apply_config()
            self.reload_api.set()
            self.log.info("reloading-finished")
        except Exception:
            self.log.critical("error-reloading", exc_info=True)
            self.terminate()
            raise

    def api_server(self):
        assert self.loop, "cannot start api server without event loop"
        self.loop.create_task(self.api_server_loop(), name="api_server_loop")

    async def api_server_loop(self):
        try:
            self.log.info(
                "api-starting", addrs=self.api_addrs, port=self.api_port
            )
            api = BackyAPI(self, self.log)
            await api.start()
            while True:
                self.log.info("api-reconfigure")
                await api.reconfigure(
                    self.api_tokens, self.api_addrs, self.api_port
                )
                self.reload_api.clear()
                await self.reload_api.wait()
        except Exception:
            self.log.exception("api_server_loop")

    def terminate(self):
        self.log.info("terminating")
        for task in asyncio.all_tasks():
            if task.get_coro().__name__ == "async_finalizer":  # type: ignore
                # Support pytest-asyncio integration.
                continue
            if task.get_coro().__name__.startswith("test_"):  # type: ignore
                # Support pytest-asyncio integration.
                continue
            self.log.debug(
                "cancelling-task",
                name=task.get_name(),
                coro_name=task.get_coro().__name__,  # type: ignore
            )
            task.cancel()

    async def shutdown_loop(self):
        assert self.loop
        self.log.debug("waiting-shutdown")
        while True:
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                break
        self.log.info("stopping-loop")
        self.loop.stop()

    class StatusDict(TypedDict):
        job: str
        sla: str
        sla_overdue: int
        status: str
        last_time: Optional[datetime.datetime]
        last_tags: Optional[str]
        last_duration: Optional[float]
        next_time: Optional[datetime.datetime]
        next_tags: Optional[str]
        manual_tags: str
        quarantine_reports: int
        unsynced_revs: int
        local_revs: int

    def status(
        self, filter_re: Optional[Pattern[str]] = None
    ) -> List[StatusDict]:
        """Collects status information for all jobs."""
        result: List["BackyDaemon.StatusDict"] = []
        for job in list(self.jobs.values()):
            if filter_re and not filter_re.search(job.name):
                continue
            job.backup.scan()
            manual_tags = set()
            unsynced_revs = 0
            history = job.backup.clean_history
            for rev in history:
                manual_tags |= filter_manual_tags(rev.tags)
                if rev.pending_changes:
                    unsynced_revs += 1
            result.append(
                dict(
                    job=job.name,
                    sla="OK" if job.sla else "TOO OLD",
                    sla_overdue=job.sla_overdue,
                    status=job.status,
                    last_time=history[-1].timestamp if history else None,
                    last_tags=(
                        ",".join(job.schedule.sorted_tags(history[-1].tags))
                        if history
                        else None
                    ),
                    last_duration=(
                        history[-1].stats.get("duration", 0)
                        if history
                        else None
                    ),
                    next_time=job.next_time,
                    next_tags=(
                        ",".join(job.schedule.sorted_tags(job.next_tags))
                        if job.next_tags
                        else None
                    ),
                    manual_tags=", ".join(manual_tags),
                    quarantine_reports=len(job.backup.quarantine.report_ids),
                    unsynced_revs=unsynced_revs,
                    local_revs=len(
                        job.backup.get_history(clean=True, local=True)
                    ),
                )
            )
        return result

    async def purge_old_files(self):
        while True:
            try:
                self.log.info("purge-scanning")
                for candidate in await aos.scandir(self.base_dir):
                    if not await is_dir_no_symlink(candidate.path):
                        continue
                    self.log.debug("purge-candidate", candidate=candidate.path)
                    reference_time = time.time() - 3 * 31 * 24 * 60 * 60
                    if not await has_recent_changes(
                        candidate.path, reference_time
                    ):
                        self.log.info("purging", candidate=candidate.path)
                        await aioshutil.rmtree(candidate)
                self.log.info("purge-finished")
            except Exception:
                self.log.exception("purge")
            await asyncio.sleep(24 * 60 * 60)

    async def purge_pending_backups(self):
        while True:
            try:
                self.log.info("purge-pending-scanning")
                for candidate in await aos.scandir(self.base_dir):
                    if (
                        candidate.name in self.jobs  # will get purged anyway
                        or not await is_dir_no_symlink(candidate.path)
                        or not await aos.path.exists(
                            p.join(candidate.path, ".purge_pending")
                        )
                    ):
                        continue
                    self.log.info("purging-pending", job=candidate.name)
                    await Job(self, candidate.name, self.log).run_purge()
                self.log.info("purge-pending-finished")
            except Exception:
                self.log.exception("purge-pending")
            await asyncio.sleep(24 * 60 * 60)


def main(config_file: Path, log: BoundLogger):  # pragma: no cover
    global daemon

    loop = asyncio.get_event_loop()
    daemon = BackyDaemon(config_file, log)
    daemon.start(loop)
    daemon.api_server()
    daemon.run_forever()
