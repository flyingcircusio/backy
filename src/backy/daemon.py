import asyncio
import fcntl
import logging
import os
import os.path as p
import re
import shutil
import signal
import sys
import time

import humanize
import pkg_resources
import prettytable
import telnetlib3
import yaml

from .schedule import Schedule
from .scheduler import Job
from .utils import SafeFile, format_datetime_local, has_recent_changes

logger = logging.getLogger(__name__)
daemon = None


class BackyDaemon(object):
    # config defaults, will be overriden from config file
    worker_limit = 1
    base_dir = None
    status_file = None
    status_interval = 30
    telnet_addrs = "::1, 127.0.0.1"
    telnet_port = 6023

    loop = None

    def __init__(self, config_file):
        self.config_file = config_file
        self.config = None
        self.schedules = {}
        self.backup_semaphores = {}
        self.jobs = {}
        self._lock = None

    def _read_config(self):
        if not p.exists(self.config_file):
            logger.error(
                "Could not load configuration. `%s` does not exist.",
                self.config_file,
            )
            raise SystemExit(1)
        with open(self.config_file, encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            self.config = yaml.safe_load(f)

        g = self.config.get("global", {})
        self.worker_limit = int(g.get("worker-limit", self.worker_limit))
        self.base_dir = g.get("base-dir", self.base_dir)
        if not self.status_file:
            self.status_file = p.join(self.base_dir, "status")
        self.status_file = g.get("status-file", self.status_file)
        self.status_interval = int(
            g.get("status-interval", self.status_interval)
        )
        self.telnet_addrs = g.get("telnet-addrs", self.telnet_addrs)
        self.telnet_port = int(g.get("telnet-port", self.telnet_port))

        new = {}
        for name, config in self.config["schedules"].items():
            if name in self.schedules:
                new[name] = self.schedules[name]
            else:
                new[name] = Schedule()
            new[name].configure(config)
        self.schedules = new

        logger.info("status interval: %s", self.status_interval)
        logger.info("status location: %s", self.status_file)
        logger.info("worker limit: %s", self.worker_limit)
        logger.info("backup location: %s", self.base_dir)
        logger.info("available schedules: %s", ", ".join(self.schedules))

    def _apply_config(self):
        # Add new jobs and update existing jobs
        for name, config in self.config["jobs"].items():
            if name not in self.jobs:
                self.jobs[name] = job = Job(self, name)
                logger.info(f"{name}: added new job")
            job = self.jobs[name]
            if config != job.last_config:
                logger.info(f"{name}: applying (changed) config")
                job.configure(config)
                job.stop()
                job.start()

        for name, job in list(self.jobs.items()):
            if name not in self.config["jobs"]:
                job.stop()
                del self.jobs[name]
                logger.info(f"{name}: stopped and deleted job")

        if (
            not self.backup_semaphores
            or self.backup_semaphores["slow"]._bound_value != self.worker_limit
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
        lockfile = p.join(self.base_dir, ".lock")
        self._lock = open(lockfile, "a+b")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except EnvironmentError as e:
            logger.critical("failed to acquire lock %s: %s", lockfile, e)
            sys.exit(69)

    def start(self, loop):
        """Starts the scheduler daemon."""
        self.loop = loop

        self._read_config()

        os.chdir(self.base_dir)
        # Ensure single daemon instance.
        self.lock()

        self._apply_config()

        loop.create_task(self.save_status_file(), name="save-status")
        loop.create_task(self.purge_old_files(), name="purge-old-files")
        loop.create_task(self.shutdown_loop(), name="shutdown-cleanup")

        def handle_signals(signum, _traceback):
            logger.info("Received signal %s", signum)

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
            signal.signal(sig, handle_signals)

    def reload(self):
        logger.info("Triggering reload")
        self._read_config()
        self._apply_config()

    def telnet_server(self):
        """Starts to listen on all configured telnet addresses."""
        assert self.loop, "cannot start telnet server without event loop"
        for addr in (a.strip() for a in self.telnet_addrs.split(",")):
            logger.info(
                "starting telnet server on %s:%i", addr, self.telnet_port
            )
            server = telnetlib3.create_server(
                host=addr, port=self.telnet_port, shell=telnet_server_shell
            )
            self.loop.create_task(
                server, name=f"telnet-server-{addr}-{self.telnet_port}"
            )

    def terminate(self):
        logger.info("Terminating all tasks")
        for task in asyncio.all_tasks():
            if task.get_coro().__name__ == "async_finalizer":
                # Support pytest-asyncio integration.
                continue
            if task.get_coro().__name__.startswith("test_"):
                # Support pytest-asyncio integration.
                continue
            print(f"Cancelling: {task.get_name()}, {task.get_coro().__name__}")
            task.cancel()

    async def shutdown_loop(self):
        logger.info("Waiting for shutdown")
        while True:
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                break
        logger.info("Stopping loop")
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
                    sla="OK" if job.sla else "TOO OLD",
                    sla_overdue=job.sla_overdue,
                    status=job.status,
                    last_time=last.timestamp if last else None,
                    last_tags=(
                        ",".join(job.schedule.sorted_tags(last.tags))
                        if last
                        else None
                    ),
                    last_duration=(
                        last.stats.get("duration", 0) if last else None
                    ),
                    next_time=job.next_time,
                    next_tags=(
                        ",".join(job.schedule.sorted_tags(job.next_tags))
                        if job.next_tags
                        else None
                    ),
                )
            )
        return result

    def _write_status_file(self):
        status = self.status()
        with SafeFile(self.status_file, sync=False) as tmp:
            tmp.protected_mode = 0o644
            tmp.open_new("w")
            yaml.safe_dump(status, tmp.f)
        for job in status:
            if not job["sla_overdue"]:
                continue
            overdue = humanize.naturaldelta(job["sla_overdue"])
            logger.warning(f'SLA violation: {job["job"]} - {overdue} overdue')

    async def save_status_file(self):
        while True:
            try:
                self._write_status_file()
            except Exception as e:  # pragma: no cover
                logger.exception(e)
            await asyncio.sleep(self.status_interval)

    async def purge_old_files(self):
        # `stat` and other file system access things are _not_
        # properly async, we might want to spawn those off into a separate
        # thread.
        while True:
            logger.info("Scanning old directories to purge")
            for candidate in os.scandir(self.base_dir):
                if not candidate.is_dir(follow_symlinks=False):
                    continue
                print(candidate)
                reference_time = time.time() - 3 * 31 * 24 * 60 * 60
                if not has_recent_changes(candidate, reference_time):
                    logger.info(f"Purging {candidate.path}")
                    shutil.rmtree(candidate)
            logger.info("Finished scanning old directories to purge")
            await asyncio.sleep(24 * 60 * 60)

    def check(self):
        self._read_config()

        if not p.exists(self.status_file):
            print(
                "UNKNOWN: No status file found at {}".format(self.status_file)
            )
            sys.exit(3)

        # The output should be relatively new. Let's say 5 min max.
        s = os.stat(self.status_file)
        if time.time() - s.st_mtime > 5 * 60:
            print("CRITICAL: Status file is older than 5 minutes")
            sys.exit(2)

        failed_jobs = []

        with open(self.status_file, encoding="utf-8") as f:
            status = yaml.safe_load(f)

        for job in status:
            if job["sla"] != "OK":
                failed_jobs.append(job)

        if failed_jobs:
            print(
                "CRITICAL: {} jobs not within SLA\n{}".format(
                    len(failed_jobs),
                    "\n".join(
                        "{} (last time: {}, overdue: {})".format(
                            f["job"], f["last_time"], f["sla_overdue"]
                        )
                        for f in failed_jobs
                    ),
                )
            )
            logging.debug("failed jobs: %r", failed_jobs)
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
    from telnetlib3.server_shell import CR, LF, readline

    writer.write(
        "backy {}".format(pkg_resources.require("backy")[0].version) + CR + LF
    )
    writer.write("Ready." + CR + LF)

    linereader = readline(reader, writer)
    linereader.send(None)

    shell = SchedulerShell(writer=writer)

    command = None
    while True:
        if command:
            writer.write(CR + LF)
        writer.write("backy> ")
        command = None
        while command is None:
            # TODO: use reader.readline()
            try:
                inp = await reader.read(1)
            except Exception:
                inp = None  # Likely a timeout
            if not inp:
                return
            command = linereader.send(inp)
        command = command.strip()
        writer.write(CR + LF)
        if command == "quit":
            writer.write("Goodbye." + CR + LF)
            break
        elif command == "help":
            writer.write("jobs [filter], status, run <job>, runall, reload")
        elif command.startswith("jobs"):
            if " " in command:
                _, filter_re = command.split(" ", maxsplit=1)
            else:
                filter_re = None
            shell.jobs(filter_re)
        elif command == "reload":
            shell.reload()
        elif command == "status":
            shell.status()
        elif command.startswith("runall"):
            shell.runall()
        elif command.startswith("run"):
            if " " in command:
                _, job = command.split(" ", maxsplit=1)
            else:
                job = None
            shell.run(job)
        elif command:
            writer.write("no such command.")
    writer.close()


class SchedulerShell(object):
    writer = None

    def __init__(self, writer):
        self.writer = writer

    def jobs(self, filter_re=None):
        """List status of all known jobs. Optionally filter by regex."""
        filter_re = re.compile(filter_re) if filter_re else None

        tz = format_datetime_local(None)[1]

        t = prettytable.PrettyTable(
            [
                "Job",
                "SLA",
                "SLA overdue",
                "Status",
                f"Last Backup ({tz.zone})",
                "Last Tags",
                "Last Duration",
                f"Next Backup ({tz.zone})",
                "Next Tags",
            ]
        )
        t.align = "l"
        t.align["Last Dur"] = "r"
        t.sortby = "Job"

        jobs = daemon.status(filter_re)
        for job in jobs:
            overdue = (
                humanize.naturaldelta(job["sla_overdue"])
                if job["sla_overdue"]
                else "-"
            )
            last_duration = (
                humanize.naturaldelta(job["last_duration"])
                if job["last_duration"]
                else "-"
            )
            last_time = format_datetime_local(job["last_time"])[0]
            next_time = format_datetime_local(job["next_time"])[0]

            t.add_row(
                [
                    job["job"],
                    job["sla"],
                    overdue,
                    job["status"],
                    last_time,
                    job["last_tags"],
                    last_duration,
                    next_time,
                    job["next_tags"],
                ]
            )

        self.writer.write(t.get_string().replace("\n", "\r\n") + "\r\n")
        self.writer.write("{} jobs shown".format(len(jobs)))

    def status(self):
        """Show job status overview"""
        t = prettytable.PrettyTable(["Status", "#"])
        state_summary = {}
        for job in daemon.jobs.values():
            state_summary.setdefault(job.status, 0)
            state_summary[job.status] += 1

        for state in sorted(state_summary):
            t.add_row([state, state_summary[state]])
        self.writer.write(t.get_string().replace("\n", "\r\n"))

    def run(self, job):
        """Show job status overview"""
        try:
            job = daemon.jobs[job]
        except KeyError:
            self.writer.write("Unknown job {}".format(job))
            return
        if not hasattr(job, "_task"):
            self.writer.write("Task not ready. Try again later.")
            return
        job.run_immediately.set()
        self.writer.write("Triggered immediate run for {}".format(job.name))

    def runall(self):
        """Show job status overview"""
        for job in daemon.jobs.values():
            if not hasattr(job, "task"):
                self.writer.write(
                    "{} not ready. Try again later.".format(job.name)
                )
                continue
            job.task.run_immediately.set()
            self.writer.write("Triggered immediate run for {}".format(job.name))

    def reload(self):
        """Reload the configuration."""
        self.writer.write("Triggering daemon reload.\r\n")
        daemon.reload()
        self.writer.write("Daemon configuration reloaded.\r\n")


def check(config_file):  # pragma: no cover
    daemon = BackyDaemon(config_file)
    daemon.check()


def main(config_file):  # pragma: no cover
    global daemon

    loop = asyncio.get_event_loop()
    daemon = BackyDaemon(config_file)
    daemon.start(loop)
    daemon.telnet_server()

    logger.info("Starting async loop.")
    loop.run_forever()
    loop.close()
