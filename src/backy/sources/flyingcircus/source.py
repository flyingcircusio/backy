import json
import time
import uuid

import consulate
from structlog.stdlib import BoundLogger

from ...timeout import TimeOut, TimeOutError
from ..ceph.source import CephRBD


class FlyingCircusRootDisk(CephRBD):
    snapshot_timeout = 90

    def __init__(self, config, log: BoundLogger):
        self.config = config
        self.vm = config["vm"]
        self.consul_acl_token = config.get("consul_acl_token")
        super(FlyingCircusRootDisk, self).__init__(config, log)
        self.log = self.log.bind(vm=self.vm, subsystem="fc-disk")

    def create_snapshot(self, name):
        consul = consulate.Consul(token=self.consul_acl_token)
        snapshot_key = "snapshot/{}".format(str(uuid.uuid4()))
        self.log.info(
            "creating-snapshot",
            snapshot_name=name,
            snapshot_key=snapshot_key,
        )

        consul.kv[snapshot_key] = {"vm": self.vm, "snapshot": name}

        time.sleep(3)
        try:
            timeout = TimeOut(
                self.snapshot_timeout, interval=2, raise_on_timeout=True
            )
            while timeout.tick():
                for snapshot in self.rbd.snap_ls(self._image_name):
                    if snapshot["name"] == name:
                        return
        except TimeOutError:
            # The VM might have been shut down. Try doing a regular Ceph
            # snapshot locally.
            super(FlyingCircusRootDisk, self).create_snapshot(name)
        except KeyboardInterrupt:
            raise
        finally:
            # In case the snapshot still gets created: the general snapshot
            # deletion code in ceph/source will clean up unused backy snapshots
            # anyway. However, we need to work a little harder to delete old
            # snapshot requests, otherwise we've sometimes seen those not
            # getting deleted and then re-created all the time.
            for key in list(consul.kv.find("snapshot/")):
                try:
                    s = consul.kv[key]
                except KeyError:
                    continue
                try:
                    s = json.loads(s)
                except json.decoder.JSONDecodeError:
                    # Clean up garbage.
                    self.log.warning(
                        "create-snapshot-removing-garbage-request",
                        snapshot_key=key,
                    )
                    del consul.kv[key]
                if s["vm"] != self.vm:
                    continue
                # The knowledge about the `backy-` prefix  isn't properly
                # encapsulated here.
                if s["snapshot"].startswith("backy-"):
                    self.log.info(
                        "create-snapshot-removing-request",
                        vm=s["vm"],
                        snapshot_name=s["snapshot"],
                        snapshot_key=key,
                    )
                    del consul.kv[key]
