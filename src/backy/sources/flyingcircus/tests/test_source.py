from backy.sources import select_source
from backy.sources.flyingcircus.source import FlyingCircusRootDisk
from unittest import mock
import backy.timeout
import consulate
import json
import pytest
import subprocess


@pytest.fixture
def fcrd():
    return FlyingCircusRootDisk({'pool': 'test', 'image': 'test01.root',
                                 'vm': 'test01', 'consul_acl_token': '12345'})


def test_select_flyingcircus_source():
    assert select_source('flyingcircus') == FlyingCircusRootDisk


def test_flyingcircus_source(fcrd):
    assert fcrd.pool == 'test'
    assert fcrd.image == 'test01.root'
    assert fcrd.vm == 'test01'
    assert fcrd.consul_acl_token == '12345'


def test_flyingcircus_source_from_cli():
    s = FlyingCircusRootDisk.config_from_cli('test/test01.root,test01')
    assert s == {
        'image': 'test01.root',
        'pool': 'test',
        'consul_acl_token': None,
        'vm': 'test01',
    }


def test_flyingcircus_source_from_cli_with_acl_token():
    s = FlyingCircusRootDisk.config_from_cli('test/test01.root,test01,asdf')
    assert s['consul_acl_token'] == 'asdf'


def test_flyingcircus_source_from_cli_invalid():
    with pytest.raises(RuntimeError) as exc:
        FlyingCircusRootDisk.config_from_cli('foobar')
    assert str(exc.value) == ('flyingcircus source must be initialized with '
                              'POOL/IMAGE,VM[,CONSUL_ACL_TOKEN')


@pytest.mark.slow
def test_flyingcircus_consul_interaction(monkeypatch, fcrd):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = ConsulKVDict()
    monkeypatch.setattr(consulate, 'Consul', consul_class)

    check_output = mock.Mock()
    check_output.side_effect = [
        b'[]',
        b'[{"name": "asdf"}]']
    monkeypatch.setattr(subprocess, 'check_output', check_output)
    fcrd.create_snapshot('asdf')


class ConsulKVDict(dict):

    def __setitem__(self, k, v):
        if not isinstance(v, bytes):
            v = json.dumps(v)
        super(ConsulKVDict, self).__setitem__(k, v)

    def find(self, prefix):
        for key in self:
            if key.startswith(prefix):
                yield key


@pytest.mark.slow
def test_flyingcircus_consul_interaction_timeout(monkeypatch, fcrd):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = ConsulKVDict()
    monkeypatch.setattr(consulate, 'Consul', consul_class)

    check_output = mock.Mock()
    check_output.side_effect = [
        b'[{"name": "bsdf"}]', b'[]', b'[]', b'[]', b'[]', b'[]']
    monkeypatch.setattr(subprocess, 'check_output', check_output)

    fcrd.snapshot_timeout = 2

    with pytest.raises(backy.timeout.TimeOutError):
        fcrd.create_snapshot('asdf')
