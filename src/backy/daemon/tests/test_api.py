import asyncio
import datetime
import os
import os.path as p
import shutil
from functools import partial
from typing import List
from unittest.mock import Mock

import pytest
import yaml
from aiohttp.test_utils import unused_port

import backy.utils
from backy import utils
from backy.daemon import BackyDaemon
from backy.revision import Revision
from backy.tests import Ellipsis


async def wait_api_ready(daemon):
    while daemon.reload_api.is_set():
        await asyncio.sleep(0.1)


@pytest.fixture
async def daemons(tmp_path, log, monkeypatch):
    daemons: list[BackyDaemon] = []  # type: ignore

    async def create_daemons(count):
        ports = [unused_port() for _ in range(count)]
        for i in range(count):
            daemon_dir = tmp_path / f"daemon{i}"
            os.mkdir(daemon_dir)
            daemon = BackyDaemon(
                daemon_dir / "config", log.bind(logger=f"server-{i}")
            )
            source = str(daemon_dir / "test01.source")
            extra_conf = {
                "api": {
                    "addrs": "localhost",
                    "port": ports[i],
                    "tokens": {
                        f"authtoken-{j}-{i}": f"server-{j}"
                        for j in range(count)
                        if i != j
                    },
                },
                "peers": {
                    f"server-{j}": {
                        "url": f"http://localhost:{ports[j]}",
                        "token": f"authtoken-{i}-{j}",
                    }
                    for j in range(count)
                    if i != j
                },
            }
            with open(str(daemon_dir / "config"), "w") as f:
                f.write(
                    f"""\
---
global:
    base-dir: {str(daemon_dir)}
schedules:
    default:
        daily:
            interval: 24h
            keep: 9
jobs:
    test01:
        source:
            type: file
            filename: {source}
        schedule: default
    foo00:
        source:
            type: file
            filename: {source}
        schedule: default
"""
                    + yaml.safe_dump(extra_conf)
                )

            with open(source, "w") as f:
                f.write("I am your father, Luke!")

            def fake_create_task(coro, *args, **kwargs):
                coro.close()
                return None

            with monkeypatch.context() as m:
                m.setattr(
                    asyncio.get_running_loop(), "create_task", fake_create_task
                )
                daemon.start(asyncio.get_running_loop())
            daemon.reload_api.set()
            daemon.api_server()

            await wait_api_ready(daemon)

            daemons.append(daemon)

        return daemons

    yield create_daemons

    for d in daemons:
        d.terminate()


def create_rev(backup, log):
    rev = Revision.create(backup, {"manual:a"}, log)
    rev.timestamp = utils.now()
    rev.stats["duration"] = 60.0
    rev.materialize()
    backup.scan()
    return rev


async def modify_authtokens(
    daemons: list[BackyDaemon],
    src: list[int],
    dest: list[int],
    allow: bool,
    bidirectional=False,
):
    for d in dest:
        for s in src:
            if allow:
                daemons[d].api_tokens[f"authtoken-{s}-{d}"] = f"server-{s}"
            else:
                daemons[d].api_tokens.pop(f"authtoken-{s}-{d}", None)
        daemons[d].reload_api.set()
        await wait_api_ready(daemons[d])
    if bidirectional:
        await modify_authtokens(daemons, dest, src, allow)


async def test_remove_peer(daemons, log):
    ds = await daemons(2)

    j0 = ds[0].jobs["test01"]
    b0 = j0.repository
    rev0 = create_rev(b0, log)

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    rev0.server = "unknown"
    rev0.materialize()
    b0.scan()

    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == []


async def test_remove_remote_backup(daemons, log):
    """delete all revs if server fails"""
    ds = await daemons(2)

    j0 = ds[0].jobs["test01"]
    b0 = j0.repository
    rev0 = create_rev(b0, log)

    j1 = ds[1].jobs["test01"]
    b1 = j1.repository
    rev1 = create_rev(b1, log)

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    # pull from active job
    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]

    del ds[1].config["jobs"]["test01"]
    ds[1]._apply_config()

    # pull from dead job
    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]

    # pull from dead job with missing dir
    shutil.rmtree(b1.path)
    # works without reloading because backup.touch() will fail
    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == [rev0.uuid]


async def test_simple_sync(daemons, log):
    """pull and push changes"""
    ds = await daemons(3)

    j0 = ds[0].jobs["test01"]
    b0 = j0.repository
    rev0 = create_rev(b0, log)

    j1 = ds[1].jobs["test01"]
    b1 = j1.repository
    rev1 = create_rev(b1, log)

    # ignore offline servers
    ds[2].api_addrs = []
    ds[2].reload_api.set()
    await wait_api_ready(ds[2])

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    # pull new rev
    await j0.pull_metadata()
    b0.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]
    new_rev1 = b0.history[1]
    assert new_rev1.repository == b0
    assert new_rev1.timestamp == rev1.timestamp
    assert new_rev1.stats == rev1.stats
    assert new_rev1.tags == rev1.tags
    assert new_rev1.orig_tags == new_rev1.tags
    assert new_rev1.trust == rev1.trust
    assert new_rev1.server == "server-1"

    # pull changed rev
    rev1.distrust()
    rev1.tags = {"manual:new"}
    rev1.write_info()
    rev1.repository.scan()

    await j0.pull_metadata()
    b0.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]
    new_rev1 = b0.history[1]
    assert new_rev1.repository == b0
    assert new_rev1.timestamp == rev1.timestamp
    assert new_rev1.stats == rev1.stats
    assert new_rev1.tags == rev1.tags
    assert new_rev1.orig_tags == rev1.tags
    assert new_rev1.trust == rev1.trust
    assert new_rev1.server == "server-1"

    # mark rev for deletion
    new_rev1.remove()
    new_rev1.repository.scan()
    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]
    assert new_rev1.tags == set()
    assert new_rev1.orig_tags == rev1.tags

    # refuse push to active job
    await j0.push_metadata()
    b0.scan()
    b1.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]
    assert [r.uuid for r in b1.history] == [rev1.uuid]
    assert b0.history[1].pending_changes

    # accept push to dead job
    del ds[1].config["jobs"]["test01"]
    ds[1]._apply_config()

    await j0.push_metadata()
    b0.scan()
    b1.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid]
    assert [r.uuid for r in b1.history] == []
    assert p.exists(p.join(j1.path, ".purge_pending"))


async def test_split_brain(daemons, log):
    """split into 2 isolated groups with 2 severs and later allow communication
    server 0 and 2 contain dead jobs
    """
    ds = await daemons(4)

    await modify_authtokens(ds, [0, 1], [2, 3], allow=False, bidirectional=True)

    js = [d.jobs["test01"] for d in ds]
    bs = [j.repository for j in js]
    revs = [create_rev(b, log) for b in bs]

    for b, r in zip(bs, revs):
        assert [r.uuid for r in b.history] == [r.uuid]

    for j in js:
        await j.pull_metadata()
        j.repository.scan()

    del ds[0].config["jobs"]["test01"]
    ds[0]._apply_config()
    del ds[2].config["jobs"]["test01"]
    ds[2]._apply_config()

    # pull from visible servers
    await js[1].pull_metadata()
    await js[3].pull_metadata()

    bs[1].scan()
    bs[3].scan()

    assert [r.uuid for r in bs[1].history] == [
        revs[0].uuid,
        revs[1].uuid,
    ]
    assert [r.uuid for r in bs[3].history] == [
        revs[2].uuid,
        revs[3].uuid,
    ]

    # every server can see server 0
    await modify_authtokens(ds, [2, 3], [0], allow=True)

    await js[3].pull_metadata()
    bs[3].scan()

    assert [r.uuid for r in bs[3].history] == [
        revs[0].uuid,
        revs[2].uuid,
        revs[3].uuid,
    ]

    # create conflicting change on local copy of rev[0]
    bs[1].tags("add", bs[1].history[0].uuid, {"manual:new1"})
    bs[3].history[0].remove()
    assert bs[1].history[0].pending_changes
    assert bs[3].history[0].pending_changes

    # first push wins
    await js[1].push_metadata()
    await js[3].push_metadata()  # fails

    bs[0].scan()
    assert bs[0].history[0].tags == {"manual:new1", "manual:a"}

    # server 3 updates copy with correct data
    await js[1].pull_metadata()
    await js[3].pull_metadata()

    bs[1].scan()
    bs[3].scan()

    assert not bs[1].history[0].pending_changes
    assert not bs[3].history[0].pending_changes

    assert [(r.uuid, r.tags) for r in bs[1].history] == [
        (revs[0].uuid, {"manual:a", "manual:new1"}),
        (revs[1].uuid, {"manual:a"}),
    ]
    assert [(r.uuid, r.tags) for r in bs[3].history] == [
        (revs[0].uuid, {"manual:a", "manual:new1"}),
        (revs[2].uuid, {"manual:a"}),
        (revs[3].uuid, {"manual:a"}),
    ]

    await modify_authtokens(
        ds, [0, 1, 2, 3], [0, 1, 2, 3], allow=True, bidirectional=True
    )

    # every server gets the same view
    await js[1].pull_metadata()
    await js[3].pull_metadata()

    bs[1].scan()
    bs[3].scan()

    assert [(r.uuid, r.tags) for r in bs[1].history] == [
        (revs[0].uuid, {"manual:a", "manual:new1"}),
        (revs[1].uuid, {"manual:a"}),
        (revs[2].uuid, {"manual:a"}),
        (revs[3].uuid, {"manual:a"}),
    ]
    assert [(r.uuid, r.tags) for r in bs[3].history] == [
        (revs[0].uuid, {"manual:a", "manual:new1"}),
        (revs[1].uuid, {"manual:a"}),
        (revs[2].uuid, {"manual:a"}),
        (revs[3].uuid, {"manual:a"}),
    ]


@pytest.fixture
def jobs_dry_run(daemons, monkeypatch, clock, log, seed_random, tz_berlin):
    async def f(start_delays):
        async def null_coroutine(*args, delay=0.1, **kw):
            await asyncio.sleep(delay)

        async def run_backup(job, tags, delta=datetime.timedelta()):
            r = Revision.create(job.repository, tags, log)
            r.timestamp = backy.utils.now() + delta
            r.stats["duration"] = 1
            r.write_info()

        # This patch causes a single run through the generator loop.
        def update_status(job, orig_update_status, status):
            orig_update_status(status)
            if status in ("finished", "failed"):
                job.stop()

        orig_delay_or_event = backy.utils.delay_or_event

        async def delay_or_event(delay, event):
            if delay == 300:  # _wait_for_leader loop
                delay = 0.1
            return await orig_delay_or_event(delay, event)

        monkeypatch.setattr(backy.utils, "delay_or_event", delay_or_event)

        ds: List[BackyDaemon] = await daemons(len(start_delays))
        jobs = [d.jobs["test01"] for d in ds]

        for job, start_delay in zip(jobs, start_delays):
            monkeypatch.setattr(job, "run_expiry", null_coroutine)
            monkeypatch.setattr(job, "run_gc", null_coroutine)
            monkeypatch.setattr(job, "run_callback", null_coroutine)
            monkeypatch.setattr(job, "run_backup", partial(run_backup, job))
            monkeypatch.setattr(job, "pull_metadata", null_coroutine)
            monkeypatch.setattr(job, "push_metadata", null_coroutine)
            monkeypatch.setattr(
                job,
                "update_status",
                partial(update_status, job, job.update_status),
            )
            monkeypatch.setattr(
                job.schedule,
                "next",
                Mock(
                    return_value=(
                        backy.utils.now()
                        + datetime.timedelta(seconds=start_delay),
                        {"daily"},
                    )
                ),
            )

        return jobs

    return f


async def test_wait_for_leader_parallel(jobs_dry_run):
    """
    server 0 (leader) completes a successful backup
    server 1 waits for server 0 until a revision was created
    """

    job0, job1 = await jobs_dry_run([0.5, 0.1])
    # server 0 is leader
    await job0.run_backup({"daily"}, delta=datetime.timedelta(hours=-1))

    utils.log_data = ""

    job0.start()
    job1.start()

    while job0._task is not None or job1._task is not None:
        await asyncio.sleep(0.1)

    assert (
        Ellipsis(
            """\
...
... AAAA I test01[A4WN]         job/waiting                         [server-0] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for deadline'
...
... AAAA I test01[N6PW]         job/waiting                         [server-1] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for deadline'
... AAAA I test01[N6PW]         job/woken                           [server-1] trigger=None
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='checking neighbours'
...
... AAAA I test01[N6PW]         job/local-revs                      [server-1] local_revs=0
... AAAA I test01[N6PW]         job/duplicate-job                   [server-1] remote_revs=1 server='server-0'
... AAAA I test01[N6PW]         job/leader-found                    [server-1] leader='server-0' leader_revs=1
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='monitoring (server-0)'
...
... AAAA I test01[A4WN]         job/woken                           [server-0] trigger=None
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='checking neighbours'
...
... AAAA I test01[A4WN]         job/local-revs                      [server-0] local_revs=1
... AAAA I test01[A4WN]         job/duplicate-job                   [server-0] remote_revs=0 server='server-1'
... AAAA I test01[A4WN]         job/leader-found                    [server-0] leader=None leader_revs=1
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for worker slot (fast)'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='running (fast)'
... AAAA D -                    revision/writing-info               revision_uuid='...' tags='daily'
...
... AAAA I test01[N6PW]         job/leader-finished                 [server-1] leader='server-0'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='finished'
...
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='finished'
...
"""
        )
        == utils.log_data
    )


async def test_wait_for_leader_delayed(jobs_dry_run):
    """
    server 1 will not wait for server 0 (leader) due to clock differences
    """

    job0, job1 = await jobs_dry_run([500, 0.1])
    # server 0 is leader
    await job0.run_backup({"daily"}, delta=datetime.timedelta(hours=-1))

    utils.log_data = ""

    job0.start()
    job1.start()

    while job1._task is not None:
        await asyncio.sleep(0.1)

    job0.stop()

    assert (
        Ellipsis(
            """\
...
... AAAA I test01[A4WN]         job/waiting                         [server-0] next_tags='daily' next_time='2015-09-01 09:15:07'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for deadline'
...
... AAAA I test01[N6PW]         job/waiting                         [server-1] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for deadline'
... AAAA I test01[N6PW]         job/woken                           [server-1] trigger=None
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='checking neighbours'
...
... AAAA I test01[N6PW]         job/local-revs                      [server-1] local_revs=0
... AAAA I test01[N6PW]         job/duplicate-job                   [server-1] remote_revs=1 server='server-0'
... AAAA I test01[N6PW]         job/leader-found                    [server-1] leader='server-0' leader_revs=1
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='monitoring (server-0)'
... AAAA I test01[N6PW]         job/leader-not-scheduled            [server-1] leader='server-0'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for worker slot (slow)'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='running (slow)'
... AAAA D -                    revision/writing-info               revision_uuid='...' tags='daily'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='finished'
...
"""
        )
        == utils.log_data
    )


async def test_wait_for_leader_crash(jobs_dry_run, monkeypatch):
    """
    server 1 will wait for server 0 (leader) until it becomes unreachable and then create a backup itself
    server 2 (offline) will be ignored
    """

    job0, job1, job2 = await jobs_dry_run([0.5, 0.1, 0])
    # server 0 is leader
    await job0.run_backup({"daily"}, delta=datetime.timedelta(hours=-1))

    job2.daemon.api_addrs = []
    job2.daemon.reload_api.set()
    await wait_api_ready(job2.daemon)

    async def crash(*args, **kw):
        # api_addrs does not work here because the connection is already established
        job0.daemon.api_tokens = {}
        job0.daemon.reload_api.set()

    monkeypatch.setattr(job0, "run_backup", crash)

    utils.log_data = ""

    job0.start()
    job1.start()

    while job1._task is not None:
        await asyncio.sleep(0.1)

    job0.stop()

    assert (
        Ellipsis(
            """\
...
... AAAA I test01[A4WN]         job/waiting                         [server-0] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for deadline'
...
... AAAA I test01[N6PW]         job/waiting                         [server-1] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for deadline'
... AAAA I test01[N6PW]         job/woken                           [server-1] trigger=None
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='checking neighbours'
...
... AAAA I test01[N6PW]         job/local-revs                      [server-1] local_revs=0
... AAAA I test01[N6PW]         job/duplicate-job                   [server-1] remote_revs=1 server='server-0'
... AAAA I test01[N6PW]         job/server-unavailable              [server-1] exception_class='aiohttp.client_exceptions.ClientConnectorError' exception_msg="Cannot connect to host ... ssl:default [Connect call failed (...)]" server='server-2'
... AAAA I test01[N6PW]         job/leader-found                    [server-1] leader='server-0' leader_revs=1
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='monitoring (server-0)'
...
... AAAA I test01[A4WN]         job/local-revs                      [server-0] local_revs=1
... AAAA I test01[A4WN]         job/duplicate-job                   [server-0] remote_revs=0 server='server-1'
... AAAA I test01[A4WN]         job/server-unavailable              [server-0] exception_class='aiohttp.client_exceptions.ClientConnectorError' exception_msg="Cannot connect to host ... ssl:default [Connect call failed (...)]" server='server-2'
... AAAA I test01[A4WN]         job/leader-found                    [server-0] leader=None leader_revs=1
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for worker slot (fast)'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='running (fast)'
... AAAA I -                    daemon/api-reconfigure              [server-0] \n\
...
... AAAA W test01[N6PW]         job/leader-failed                   [server-1] exception_class='aiohttp.client_exceptions.ClientResponseError' exception_msg="401, message='Unauthorized', url=URL('...')" leader='server-0'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for worker slot (slow)'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='running (slow)'
... AAAA D -                    revision/writing-info               revision_uuid='...' tags='daily'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='finished'
...
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='finished'
...
"""
        )
        == utils.log_data
    )


async def test_wait_for_leader_stopped(jobs_dry_run):
    """
    server 1 will ignore server 0 (leader, with a stopped job)
    """

    job0, job1 = await jobs_dry_run([0.5, 0.1])
    # server 0 is leader
    await job0.run_backup({"daily"}, delta=datetime.timedelta(hours=-1))

    utils.log_data = ""

    job1.start()

    while job1._task is not None:
        await asyncio.sleep(0.1)

    assert (
        Ellipsis(
            """\
...
... AAAA I test01[A4WN]         job/waiting                         [server-1] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[A4WN]         job/updating-status                 [server-1] status='waiting for deadline'
... AAAA I test01[A4WN]         job/woken                           [server-1] trigger=None
... AAAA D test01[A4WN]         job/updating-status                 [server-1] status='checking neighbours'
...
... AAAA I test01[A4WN]         job/local-revs                      [server-1] local_revs=0
... AAAA I test01[A4WN]         job/duplicate-job                   [server-1] remote_revs=1 server='server-0'
... AAAA I test01[A4WN]         job/leader-found                    [server-1] leader='server-0' leader_revs=1
... AAAA D test01[A4WN]         job/updating-status                 [server-1] status='monitoring (server-0)'
... AAAA I test01[A4WN]         job/leader-stopped                  [server-1] leader='server-0'
... AAAA D test01[A4WN]         job/updating-status                 [server-1] status='waiting for worker slot (slow)'
... AAAA D test01[A4WN]         job/updating-status                 [server-1] status='running (slow)'
... AAAA D -                    revision/writing-info               revision_uuid='...' tags='daily'
... AAAA D test01[A4WN]         job/updating-status                 [server-1] status='finished'
...
"""
        )
        == utils.log_data
    )


async def test_wait_for_leader_ambiguous_leader(jobs_dry_run, monkeypatch):
    """
    server 0 and server 1 have the same amount of revisions and will both create a new one
    """

    job0, job1 = await jobs_dry_run([0.7, 0.0])

    async def noop(*args, **kw):
        # do not create a rev
        pass

    monkeypatch.setattr(job1, "run_backup", noop)

    utils.log_data = ""

    job0.start()
    job1.start()

    while job0._task is not None or job1._task is not None:
        await asyncio.sleep(0.1)

    assert (
        Ellipsis(
            """\
...
... AAAA I test01[A4WN]         job/waiting                         [server-0] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for deadline'
...
... AAAA I test01[N6PW]         job/waiting                         [server-1] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for deadline'
... AAAA I test01[N6PW]         job/woken                           [server-1] trigger=None
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='checking neighbours'
...
... AAAA I test01[N6PW]         job/local-revs                      [server-1] local_revs=0
... AAAA I test01[N6PW]         job/duplicate-job                   [server-1] remote_revs=0 server='server-0'
... AAAA I test01[N6PW]         job/leader-found                    [server-1] leader=None leader_revs=0
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for worker slot (slow)'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='running (slow)'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='finished'
...
... AAAA I test01[A4WN]         job/woken                           [server-0] trigger=None
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='checking neighbours'
...
... AAAA I test01[A4WN]         job/local-revs                      [server-0] local_revs=0
... AAAA I test01[A4WN]         job/duplicate-job                   [server-0] remote_revs=0 server='server-1'
... AAAA I test01[A4WN]         job/leader-found                    [server-0] leader=None leader_revs=0
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for worker slot (slow)'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='running (slow)'
... AAAA D -                    revision/writing-info               revision_uuid='...' tags='daily'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='finished'
...
"""
        )
        == utils.log_data
    )


async def test_wait_for_leader_run_immediately(jobs_dry_run):
    """
    sever 1 will stop waiting on signal
    """

    job0, job1 = await jobs_dry_run([10, 0.1])
    # server 0 is leader
    await job0.run_backup({"daily"}, delta=datetime.timedelta(hours=-1))

    utils.log_data = ""

    job0.start()
    job1.start()

    await asyncio.sleep(0.5)
    job1.run_immediately.set()

    while job1._task is not None:
        await asyncio.sleep(0.1)

    job0.stop()

    assert (
        Ellipsis(
            """\
...
... AAAA I test01[A4WN]         job/waiting                         [server-0] next_tags='daily' next_time='2015-09-01 09:06:57'
... AAAA D test01[A4WN]         job/updating-status                 [server-0] status='waiting for deadline'
...
... AAAA I test01[N6PW]         job/waiting                         [server-1] next_tags='daily' next_time='2015-09-01 09:06:47'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for deadline'
... AAAA I test01[N6PW]         job/woken                           [server-1] trigger=None
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='checking neighbours'
...
... AAAA I test01[N6PW]         job/local-revs                      [server-1] local_revs=0
... AAAA I test01[N6PW]         job/duplicate-job                   [server-1] remote_revs=1 server='server-0'
... AAAA I test01[N6PW]         job/leader-found                    [server-1] leader='server-0' leader_revs=1
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='monitoring (server-0)'
...
... AAAA I test01[N6PW]         job/run-immediately-triggered       [server-1] leader='server-0'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='waiting for worker slot (slow)'
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='running (slow)'
...
... AAAA D test01[N6PW]         job/updating-status                 [server-1] status='finished'
...
"""
        )
        == utils.log_data
    )
