from backy.sources.ceph.source import CephRBD
from backy.timeout import TimeOut
import consulate
import logging
import uuid


logger = logging.getLogger(__name__)


class FlyingCircusRootDisk(CephRBD):

    snapshot_timeout = 30

    def __init__(self, config):
        self.config = config
        self.vm = config['vm']
        config['pool'] = config['vm'][:-2]
        config['image'] = config['vm'] + '.root'
        super(FlyingCircusRootDisk, self).__init__(config)

    @staticmethod
    def config_from_cli(spec):
        return dict(vm=spec)

    def _create_snapshot(self, name):
        consul = consulate.Consul(
            token=self.config['consul_acl_token'])
        snapshot_key = 'snapshot/{}'.format(str(uuid.uuid4()))
        logger.info('Requesting consistent snapshot via {} ...'.
                    format(snapshot_key))
        consul.kv[snapshot_key] = {'vm': self.vm, 'snapshot': name}
        try:
            timeout = TimeOut(self.snapshot_timeout,
                              raise_on_timeout=True)
            while timeout.tick():
                for snapshot in self.rbd.snap_ls(self._image_name):
                    if snapshot['name'] == name:
                        return
        finally:
            # In case the snapshot still gets created: the general snapshot
            # deletion code in ceph/source will clean up unused backy snapshots
            # anyway.
            del consul.kv[snapshot_key]
