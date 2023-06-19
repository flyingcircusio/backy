import asyncio
import os
import os.path as p
import shutil

import pytest
import yaml
from aiohttp.test_utils import unused_port

from backy import utils
from backy.daemon import BackyDaemon
from backy.revision import Revision


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
    b0 = j0.backup
    rev0 = create_rev(b0, log)

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    rev0.location = "unknown"
    rev0.materialize()
    b0.scan()

    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == []


async def test_remove_remote_backup(daemons, log):
    ds = await daemons(2)

    j0 = ds[0].jobs["test01"]
    b0 = j0.backup
    rev0 = create_rev(b0, log)

    j1 = ds[1].jobs["test01"]
    b1 = j1.backup
    rev1 = create_rev(b1, log)

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    del ds[1].jobs["test01"]
    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]

    shutil.rmtree(b1.path)
    await j0.pull_metadata()
    b0.scan()
    assert [r.uuid for r in b0.history] == [rev0.uuid]


async def test_simple_sync(daemons, log):
    ds = await daemons(3)

    j0 = ds[0].jobs["test01"]
    b0 = j0.backup
    rev0 = create_rev(b0, log)

    j1 = ds[1].jobs["test01"]
    b1 = j1.backup
    rev1 = create_rev(b1, log)

    ds[2].api_addrs = []
    ds[2].reload_api.set()
    await wait_api_ready(ds[2])

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    await j0.pull_metadata()
    b0.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid]

    del ds[1].jobs["test01"]
    await j0.pull_metadata()
    b0.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]
    new_rev1 = b0.history[1]
    assert new_rev1.backup == b0
    assert new_rev1.timestamp == rev1.timestamp
    assert new_rev1.backend_type == ""
    assert new_rev1.stats == rev1.stats
    assert new_rev1.tags == rev1.tags
    assert new_rev1.orig_tags == new_rev1.tags
    assert new_rev1.trust == rev1.trust
    assert new_rev1.location == "server-1"

    new_rev1.remove()
    assert [r.uuid for r in b0.history] == [rev0.uuid, rev1.uuid]
    assert new_rev1.tags == set()
    assert new_rev1.orig_tags == rev1.tags

    await j0.push_metadata()
    b0.scan()
    b1.scan()

    assert [r.uuid for r in b0.history] == [rev0.uuid]
    assert [r.uuid for r in b1.history] == []
    assert p.exists(p.join(j1.path, ".purge_pending"))


async def test_split_brain(daemons, log):
    ds = await daemons(4)

    await modify_authtokens(ds, [0, 1], [2, 3], allow=False, bidirectional=True)

    js = [d.jobs["test01"] for d in ds]
    bs = [j.backup for j in js]
    revs = [create_rev(b, log) for b in bs]

    for b, r in zip(bs, revs):
        assert [r.uuid for r in b.history] == [r.uuid]

    for j in js:
        await j.pull_metadata()
        j.backup.scan()

    for b, r in zip(bs, revs):
        assert [r.uuid for r in b.history] == [r.uuid]

    del ds[0].jobs["test01"]
    del ds[2].jobs["test01"]

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

    await modify_authtokens(ds, [2, 3], [0], allow=True)

    await js[3].pull_metadata()
    bs[3].scan()

    assert [r.uuid for r in bs[3].history] == [
        revs[0].uuid,
        revs[2].uuid,
        revs[3].uuid,
    ]

    bs[1].history[0].tags.add("manual:new1")
    bs[3].history[0].remove()

    await js[1].push_metadata()
    await js[3].push_metadata()  # fails

    bs[0].scan()
    assert bs[0].history[0].tags == {"manual:new1", "manual:a"}

    await js[1].pull_metadata()
    await js[3].pull_metadata()

    bs[1].scan()
    bs[3].scan()

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

    await js[1].pull_metadata()
    await js[3].pull_metadata()

    bs[1].scan()
    bs[3].scan()

    assert [(r.uuid, r.tags) for r in bs[1].history] == [
        (revs[0].uuid, {"manual:a", "manual:new1"}),
        (revs[1].uuid, {"manual:a"}),
        (revs[2].uuid, {"manual:a"}),
    ]
    assert [(r.uuid, r.tags) for r in bs[3].history] == [
        (revs[0].uuid, {"manual:a", "manual:new1"}),
        (revs[2].uuid, {"manual:a"}),
        (revs[3].uuid, {"manual:a"}),
    ]
