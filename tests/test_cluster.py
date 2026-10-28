import pytest

from dcache.cluster import CacheUnavailable, DistributedCache
from dcache.lru import LRUCache

KEYS = [f"k{i}" for i in range(300)]


def test_set_and_get(cache):
    assert cache.set("user:1", "alice") == 2
    assert cache.get("user:1") == "alice"
    assert cache.get("nope") is None
    assert cache.get("nope", "dflt") == "dflt"


def test_each_key_is_stored_on_exactly_its_replicas(cache, fake):
    for k in KEYS:
        cache.set(k, "v")
    for k in KEYS:
        holders = {n for n in fake.servers if fake.clients[n].exists(f"dc:{k}")}
        assert holders == set(cache.replica_nodes(k))


def test_keys_are_spread_over_all_nodes(cache, fake):
    for k in KEYS:
        cache.set(k, "v")
    # 300 keys x 2 replicas over 3 nodes: about 200 each.
    for n in fake.servers:
        assert 120 < len(fake.keys_on(n)) < 280


def test_ttl_is_applied_on_every_replica(cache, fake):
    cache.set("session", "s", ttl=30)
    for n in cache.replica_nodes("session"):
        assert 29_000 < fake.clients[n].pttl("dc:session") <= 30_000


def test_delete_removes_all_replicas(cache, fake):
    cache.set("k", "v")
    assert cache.delete("k")
    assert cache.get("k") is None
    assert not any(fake.clients[n].exists("dc:k") for n in fake.servers)


def test_read_fails_over_to_replica_when_primary_is_down(cache, fake):
    for k in KEYS:
        cache.set(k, f"v-{k}")
    fake.kill("n1")
    for k in KEYS:
        assert cache.get(k) == f"v-{k}"      # every key still readable
    assert cache.stats["node_errors"] > 0


def test_write_succeeds_with_one_replica_down(cache, fake):
    fake.kill("n2")
    for k in KEYS:
        assert cache.set(k, "v") >= 1
        assert cache.get(k) == "v"


def test_write_quorum_is_enforced(fake):
    strict = DistributedCache(fake.clients, replicas=2, write_quorum=2)
    key = next(k for k in KEYS if "n1" in strict.replica_nodes(k))
    fake.kill("n1")
    with pytest.raises(CacheUnavailable):
        strict.set(key, "v")


def test_read_repair_restores_a_missing_replica(cache, fake):
    cache.set("k", "v", ttl=60)
    primary, replica = cache.replica_nodes("k")
    fake.clients[primary].delete("dc:k")     # e.g. primary restarted empty
    assert cache.get("k") == "v"
    assert fake.clients[primary].get("dc:k") == "v"
    assert fake.clients[primary].pttl("dc:k") > 0   # TTL copied too
    assert cache.stats["read_repairs"] == 1


def test_all_replicas_down_is_a_miss_not_a_crash(cache, fake):
    cache.set("k", "v")
    for n in fake.servers:
        fake.kill(n)
    assert cache.get("k") is None
    with pytest.raises(CacheUnavailable):
        cache.set("k", "v2")


def test_near_cache_serves_repeat_reads_and_is_invalidated(fake):
    near = LRUCache(100, default_ttl=5)
    c = DistributedCache(fake.clients, replicas=2, near_cache=near)
    c.set("k", "v1")
    assert c.get("k") == "v1"
    assert c.get("k") == "v1"
    assert c.stats["near_hits"] == 1
    c.set("k", "v2")                          # local write invalidates
    assert c.get("k") == "v2"
    c.delete("k")
    assert c.get("k") is None


def test_rejects_bad_quorum(fake):
    with pytest.raises(ValueError):
        DistributedCache(fake.clients, replicas=2, write_quorum=3)
