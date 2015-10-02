from backy.sources.flyingcircus.source import FlyingCircusRootDisk
import backy.timeout
import consulate
from unittest import mock
import pytest
import subprocess


def test_flyingcircus_source():
    s = FlyingCircusRootDisk(dict(vm='test01'))
    assert s.pool == 'test'
    assert s.image == 'test01.root'
    assert s.vm == 'test01'


def test_flyingcircus_source_from_cli():
    s = FlyingCircusRootDisk.config_from_cli('test01')
    assert s == {'vm': 'test01'}


def test_flyingcircus_consul_interaction(monkeypatch):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = {}
    monkeypatch.setattr(consulate, 'Consul', consul_class)

    check_output = mock.Mock()
    check_output.side_effect = [
        b'[]',
        b'[{"name": "asdf"}]']
    monkeypatch.setattr(subprocess, 'check_output', check_output)

    s = FlyingCircusRootDisk(dict(vm='test01'))
    s.config['consul_acl_token'] = '12345'
    s._create_snapshot('asdf')


@pytest.mark.slow
def test_flyingcircus_consul_interaction_timeout(monkeypatch):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = {}
    monkeypatch.setattr(consulate, 'Consul', consul_class)

    check_output = mock.Mock()
    check_output.side_effect = [
        b'[{"name": "bsdf"}]', b'[]', b'[]', b'[]', b'[]', b'[]']
    monkeypatch.setattr(subprocess, 'check_output', check_output)

    s = FlyingCircusRootDisk(dict(vm='test01'))
    s.snapshot_timeout = 2
    s.config['consul_acl_token'] = '12345'

    with pytest.raises(backy.timeout.TimeOutError):
        s._create_snapshot('asdf')
