from ...timeout import TimeOut
from ..ceph.source import CephRBD
import consulate
import json
import logging
import time
import uuid


logger = logging.getLogger(__name__)


class FlyingCircusRootDisk(CephRBD):

    snapshot_timeout = 90

    def __init__(self, config):
        self.config = config
        self.vm = config['vm']
        self.consul_acl_token = config.get('consul_acl_token')
        super(FlyingCircusRootDisk, self).__init__(config)

    @classmethod
    def config_from_cli(cls, spec):
        logger.debug('FlyingCircusRootDisk.config_from_cli(%s)', spec)
        param = [v.strip() for v in spec.split(',')]
        if len(param) not in [2, 3]:
            raise RuntimeError('flyingcircus source must be initialized with '
                               'POOL/IMAGE,VM[,CONSUL_ACL_TOKEN')
        volume, vm = param[:2]
        consul_acl_token = param[2] if len(param) == 3 else None
        c = super(FlyingCircusRootDisk, cls).config_from_cli(volume)
        c['vm'] = vm
        c['consul_acl_token'] = consul_acl_token
        return c

    def create_snapshot(self, name):
        consul = consulate.Consul(token=self.consul_acl_token)
        snapshot_key = 'snapshot/{}'.format(str(uuid.uuid4()))
        logger.info('Consul: requesting consistent snapshot of %s@%s via %s',
                    self.vm, name, snapshot_key)

        consul.kv[snapshot_key] = {'vm': self.vm, 'snapshot': name}

        time.sleep(3)
        try:
            timeout = TimeOut(self.snapshot_timeout, interval=2,
                              raise_on_timeout=True)
            while timeout.tick():
                for snapshot in self.rbd.snap_ls(self._image_name):
                    if snapshot['name'] == name:
                        return
        finally:
            # In case the snapshot still gets created: the general snapshot
            # deletion code in ceph/source will clean up unused backy snapshots
            # anyway. However, we need to work a little harder to delete old
            # snapshot requests, otherwise we've seen those sometimes not
            # getting deleted and then re-created all the time.
            for key in list(consul.kv.find('snapshot/')):
                try:
                    s = consul.kv[key]
                except AttributeError:
                    continue
                s = json.loads(s)
                if s['vm'] != self.vm:
                    continue
                # The knowledge about the `backy-` prefix  isn't properly
                # encapsulated here.
                if s['snapshot'].startswith('backy-'):
                    logger.info(
                        'Consul: removing snapshot request of %s@%s via %s',
                        s['vm'], s['snapshot'], key)
                    del consul.kv[key]
