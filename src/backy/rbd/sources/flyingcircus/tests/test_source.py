import json
import subprocess
from unittest import mock

import consulate
import pytest

from backy.sources import select_source
from backy.sources.flyingcircus.source import FlyingCircusRootDisk


@pytest.fixture
def fcrd(log):
    return FlyingCircusRootDisk(
        {
            "pool": "test",
            "image": "test01.root",
            "vm": "test01",
            "consul_acl_token": "12345",
        },
        log,
    )


def test_select_flyingcircus_source():
    assert select_source("flyingcircus") == FlyingCircusRootDisk


def test_flyingcircus_source(fcrd):
    assert fcrd.pool == "test"
    assert fcrd.image == "test01.root"
    assert fcrd.vm == "test01"
    assert fcrd.consul_acl_token == "12345"


@pytest.mark.slow
def test_flyingcircus_consul_interaction(monkeypatch, fcrd):
    consul_class = mock.Mock()
    consul = consul_class()
    consul.kv = ConsulKVDict()
    monkeypatch.setattr(consulate, "Consul", consul_class)

    check_output = mock.Mock()
    check_output.side_effect = ["[]", '[{"name": "asdf"}]']
    monkeypatch.setattr(subprocess, "check_output", check_output)
    fcrd.create_snapshot("asdf")


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
    monkeypatch.setattr(consulate, "Consul", consul_class)

    check_output = mock.Mock()
    check_output.side_effect = [
        '[{"name": "bsdf"}]',
        "[]",
        "[]",
        "[]",
        "[]",
        "[]",
    ]
    monkeypatch.setattr(subprocess, "check_output", check_output)

    fcrd.snapshot_timeout = 2
    fcrd.create_snapshot("asdf")

    assert check_output.call_args[0][0] == [
        "rbd",
        "snap",
        "create",
        "test/test01.root@asdf",
    ]
