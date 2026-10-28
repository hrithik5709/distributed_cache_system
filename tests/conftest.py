import fakeredis
import pytest

from dcache.cluster import DistributedCache


class FakeCluster:
    """N independent fake Redis servers, each of which can be taken down."""

    def __init__(self, names):
        self.servers = {n: fakeredis.FakeServer() for n in names}
        self.clients = {n: self.client(n) for n in names}

    def client(self, name):
        return fakeredis.FakeRedis(server=self.servers[name], decode_responses=True)

    def kill(self, name):
        self.servers[name].connected = False

    def revive(self, name):
        self.servers[name].connected = True

    def keys_on(self, name):
        return set(self.clients[name].scan_iter(match="dc:*"))


@pytest.fixture
def fake():
    return FakeCluster(["n1", "n2", "n3"])


@pytest.fixture
def cache(fake):
    return DistributedCache(fake.clients, replicas=2, failure_threshold=2)
