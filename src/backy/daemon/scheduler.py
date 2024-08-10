import asyncio
import datetime
import hashlib
import random
import subprocess
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, List, Literal, Optional, Set

import yaml
from aiohttp import ClientConnectionError, ClientError, ClientResponseError
from aiohttp.web_exceptions import HTTPForbidden, HTTPNotFound
from structlog.stdlib import BoundLogger

import backy.utils
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule
from backy.utils import format_datetime_local, generate_taskid, time_or_event

from ..source import AsyncCmdLineSource
from .api import Client, ClientManager

if TYPE_CHECKING:
    from backy.daemon import BackyDaemon
    from backy.repository import StatusDict


def locked(target: str, mode: Literal["shared", "exclusive"]):
    return Repository.locked(target, mode, repo_attr="repository")


class Job(object):
    name: str
    source: AsyncCmdLineSource
    status: str = ""
    next_time: Optional[datetime.datetime] = None
    next_tags: Optional[set[str]] = None
    path: Path
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
        repository = Repository(
            self.path, self.daemon.schedules[config["schedule"]], self.log
        )
        repository.connect()
        self.source = AsyncCmdLineSource(repository, config["source"], self.log)
        self.source.store()
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
    def schedule(self) -> Schedule:
        return self.repository.schedule

    @property
    def repository(self) -> Repository:
        return self.source.repository

    def update_status(self, status: str) -> None:
        self.status = status
        self.log.debug("updating-status", status=self.status)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "path": self.path,
            "status": self.status,
            # "source": self.source,
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
            api = ClientManager(self.daemon.peers, self.taskid, self.log)
            statuses = await asyncio.gather(
                *[api[server].fetch_status(f"^{self.name}$") for server in api],
                return_exceptions=True,
            )
            leader = None
            leader_revs = len(
                self.repository.get_history(clean=True, local=True)
            )
            leader_status: "StatusDict"
            self.log.info("local-revs", local_revs=leader_revs)
            for server, status in zip(api, statuses):
                log = self.log.bind(server=server)
                if isinstance(status, BaseException):
                    log.info(
                        "server-unavailable",
                        exc_info=status,
                        exc_style="short",
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
            # TODO: use contextvars
            self.log = self.log.bind(job_name=self.name, sub_taskid=self.taskid)

            self.source.log = self.source.log.bind(
                job_name=self.name, sub_taskid=self.taskid
            )
            self.repository.log = self.repository.log.bind(
                job_name=self.name, sub_taskid=self.taskid
            )
            self.repository.connect()

            next_time, next_tags = self.schedule.next(
                backy.utils.now(), self.spread, self.repository
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
                        self.repository.clean_history
                        and self.repository.clean_history[-1].stats["duration"]
                        < 600
                    ):
                        speed = "fast"
                    self.update_status(f"waiting for worker slot ({speed})")

                    async with self.daemon.backup_semaphores[speed]:
                        self.update_status(f"running ({speed})")

                        self.repository._clean()
                        await self.run_backup(next_tags)
                        self.repository.scan()
                        self.repository._clean()
                        await self.pull_metadata()
                        await self.run_expiry()
                        await self.push_metadata()
                        await self.run_gc()
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

    async def run_backup(self, tags: Set[str]) -> None:
        self.log.info("backup-started", tags=", ".join(tags))

        r = Revision.create(self.repository, tags, self.log)
        r.materialize()
        return_code = await self.source.backup(r)
        if return_code:
            raise RuntimeError(f"Backup failed with return code {return_code}")

    async def run_expiry(self) -> None:
        self.log.info("expiry-started")
        # includes lock and repository.scan()
        self.repository.expire()

    async def run_gc(self) -> None:
        self.log.info("gc-started")
        await self.source.gc()

    async def run_callback(self) -> None:
        if not self.daemon.backup_completed_callback:
            self.log.debug("callback-not-configured")
            return

        self.log.info("callback-started")
        status = yaml.safe_dump(
            [r.to_dict() for r in self.repository.history]
        ).encode("utf-8")

        callback_proc = await asyncio.create_subprocess_exec(
            str(self.daemon.backup_completed_callback),
            self.name,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = await callback_proc.communicate(status)
            self.log.info(
                "callback-finished",
                return_code=callback_proc.returncode,
                subprocess_pid=callback_proc.pid,
                stdout=stdout.decode() if stdout else None,
                stderr=stderr.decode() if stderr else None,
            )
        except asyncio.CancelledError:
            self.log.warning(
                "callback-cancelled",
                subprocess_pid=callback_proc.pid,
            )
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

    @locked(target=".backup", mode="exclusive")
    async def push_metadata(self) -> int:
        grouped = defaultdict(list)
        for r in self.repository.clean_history:
            if r.pending_changes:
                grouped[r.server].append(r)
        self.log.info(
            "push-start", changes=sum(len(L) for L in grouped.values())
        )
        async with ClientManager(
            self.daemon.peers, self.taskid, self.log
        ) as apis:
            errors = await asyncio.gather(
                *[
                    self._push_metadata_single(apis[server], grouped[server])
                    for server in apis
                ]
            )
        self.log.info("push-end", errors=sum(errors))
        return sum(errors)

    async def _push_metadata_single(
        self, api: Client, revs: List[Revision]
    ) -> bool:
        purge_required = False
        error = False
        for r in revs:
            log = self.log.bind(
                server=r.server,
                rev_uuid=r.uuid,
            )
            log.debug(
                "push-updating-tags",
                old_tags=r.orig_tags,
                new_tags=r.tags,
            )
            try:
                await api.put_tags(r, autoremove=True)
                if r.tags:
                    r.orig_tags = r.tags
                    r.write_info()
                else:
                    r.remove(force=True)
                    purge_required = True
            except ClientResponseError:
                log.warning("push-client-error", exc_style="short")
                error = True
            except ClientConnectionError:
                log.warning("push-connection-error", exc_style="short")
                error = True
            except ClientError:
                log.exception("push-error")
                error = True

        if purge_required:
            log = self.log.bind(server=api.server_name)
            log.debug("push-purging-remote")
            try:
                await api.run_purge(self.name)
            except ClientResponseError:
                log.warning("push-purge-client-error", exc_style="short")
                error = True
            except ClientConnectionError:
                log.warning("push-purge-connection-error", exc_style="short")
                error = True
            except ClientError:
                log.error("push-purge-error")
                error = True
        return error

    @locked(target=".backup", mode="exclusive")
    async def pull_metadata(self) -> int:
        async def remove_dead_peer():
            for r in list(self.repository.history):
                if r.server and r.server not in self.daemon.peers:
                    self.log.info(
                        "pull-removing-dead-peer",
                        rev_uuid=r.uuid,
                        server=r.server,
                    )
                    r.remove(force=True)
            return False

        self.log.info("pull-start")
        async with ClientManager(
            self.daemon.peers, self.taskid, self.log
        ) as apis:
            errors = await asyncio.gather(
                remove_dead_peer(),
                *[self._pull_metadata_single(apis[server]) for server in apis],
            )
        self.log.info("pull-end", errors=sum(errors))
        return sum(errors)

    async def _pull_metadata_single(self, api: Client) -> bool:
        error = False
        log = self.log.bind(server=api.server_name)
        try:
            await api.touch_backup(self.name)
            remote_revs = await api.get_revs(self.repository)
            log.debug("pull-found-revs", revs=len(remote_revs))
        except ClientResponseError as e:
            if e.status in [
                HTTPNotFound.status_code,
                HTTPForbidden.status_code,
            ]:
                log.debug("pull-not-found")
            else:
                log.warning("pull-client-error", exc_style="short")
                error = True
            remote_revs = []
        except ClientConnectionError:
            log.warning("pull-connection-error", exc_style="short")
            return True
        except ClientError:
            log.exception("pull-error")
            error = True
            remote_revs = []

        local_uuids = {
            r.uuid
            for r in self.repository.history
            if r.server == api.server_name
        }
        remote_uuids = {r.uuid for r in remote_revs}
        for uuid in local_uuids - remote_uuids:
            log.warning("pull-removing-unknown-rev", rev_uuid=uuid)
            self.repository.find_by_uuid(uuid).remove(force=True)

        for r in remote_revs:
            if r.uuid in local_uuids:
                if (
                    r.to_dict()
                    == self.repository.find_by_uuid(r.uuid).to_dict()
                ):
                    continue
                log.debug("pull-updating-rev", rev_uid=r.uuid)
            else:
                log.debug("pull-new-rev", rev_uid=r.uuid)
            r.write_info()

        return error
