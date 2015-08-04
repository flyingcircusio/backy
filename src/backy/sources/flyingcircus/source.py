from backy.sources.ceph.source import CephRBD
import consulate
import uuid
from backy.timeout import TimeOut


class FlyingCircusRootDisk(CephRBD):

    def __init__(self, config):
        super(FlyingCircusRootDisk, self).__init__(config)
        self.vm = config['vm']

    @staticmethod
    def config_from_cli(spec):
        return dict(vm=spec, pool=spec[:-2], image=spec + '.root')

    def _create_snapshot(self, name):
        consul = consulate.Consul()
        snapshot_key = 'snapshot/{}'.format(str(uuid.uuid4()))
        consul.kv[snapshot_key] = {'vm': self.vm, 'name': name}
        try:
            timeout = TimeOut(30, raise_on_timeout=True)
            while timeout.tick():
                for snapshot in self.rbd.snap_ls(self._image_name):
                    if snapshot['name'] == name:
                        break
        finally:
            # In case the snapshot still gets created: the general snapshot
            # deletion code in ceph/source will clean up unused backy snapshots
            # anyway.
            del consul.kv[snapshot_key]
