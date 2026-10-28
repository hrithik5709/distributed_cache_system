import time

import fakeredis

from dcache.cluster import DistributedCache

KEYS = [f"k{i}" for i in range(400)]


def assert_fully_replicated(cache, fake):
    """Every key is on exactly its current replica set, with the right value."""
    for k in KEYS:
        holders = {n for n in cache.healthy_nodes if fake.clients[n].exists(f"dc:{k}")}
        assert holders == set(cache.replica_nodes(k)), k
        for n in holders:
            assert fake.clients[n].get(f"dc:{k}") == f"v-{k}"


def load(cache):
    for k in KEYS:
        cache.set(k, f"v-{k}")


def test_failed_node_is_removed_after_threshold(cache, fake):
    load(cache)
    fake.kill("n2")
    cache.check_health()
    assert "n2" in cache.healthy_nodes          # one failure is not enough
    cache.check_health()
    assert cache.down_nodes == {"n2"}
    assert "n2" not in cache.healthy_nodes


def test_failure_detected_from_regular_traffic(cache, fake):
    load(cache)
    fake.kill("n3")
    for k in KEYS:
        assert cache.get(k) == f"v-{k}"
    assert cache.down_nodes == {"n3"}


def test_keys_are_re_replicated_after_a_node_dies(cache, fake):
    load(cache)
    fake.kill("n1")
    cache.check_health()
    stats = cache.check_health()                # second failure: down + rebalance
    assert stats is not None and stats["copied"] > 0
    assert_fully_replicated(cache, fake)        # 2 copies each on n2 and n3
    for k in KEYS:
        assert cache.get(k) == f"v-{k}"


def test_recovered_node_is_wiped_and_rejoins(cache, fake):
    load(cache)
    fake.kill("n1")
    cache.check_health()
    cache.check_health()
    # While n1 is down, a key changes and another is deleted.
    on_n1 = [k for k in KEYS if "n1" in DistributedCache(fake.clients).replica_nodes(k)]
    changed, deleted = on_n1[0], on_n1[1]
    cache.set(changed, "new")
    cache.delete(deleted)
    fake.revive("n1")
    cache.check_health()
    assert cache.down_nodes == set() and "n1" in cache.healthy_nodes
    # n1 must not serve its stale copy or resurrect the deleted key.
    assert cache.get(changed) == "new"
    assert cache.get(deleted) is None
    for n in cache.replica_nodes(changed):
        assert fake.clients[n].get(f"dc:{changed}") == "new"


def test_adding_a_node_moves_about_a_quarter_of_keys(cache, fake):
    load(cache)
    fake.servers["n4"] = fakeredis.FakeServer()
    fake.clients["n4"] = fake.client("n4")
    stats = cache.add_node("n4", fake.clients["n4"])
    # 400 keys x 2 replicas = 800 copies; ideally 1/4 of them move to n4.
    assert 120 < stats["copied"] < 280
    assert stats["copied"] == stats["removed"]
    assert_fully_replicated(cache, fake)


def test_planned_removal_keeps_every_key(cache, fake):
    load(cache)
    cache.remove_node("n2")
    assert cache.healthy_nodes == {"n1", "n3"}
    assert_fully_replicated(cache, fake)
    assert fake.keys_on("n2") == set()          # drained


def test_rebalance_never_deletes_when_an_owner_is_unreachable(fake):
    cache = DistributedCache(fake.clients, replicas=2, failure_threshold=100)
    load(cache)
    fake.servers["n4"] = fakeredis.FakeServer()
    fake.clients["n4"] = fake.client("n4")
    fake.servers["n4"].connected = False         # new node unreachable
    cache.add_node("n4", fake.clients["n4"])
    for k in KEYS:                               # nothing was lost
        assert any(fake.clients[n].get(f"dc:{k}") == f"v-{k}" for n in ("n1", "n2", "n3"))


def test_last_node_is_never_removed(fake):
    cache = DistributedCache({"n1": fake.clients["n1"]}, replicas=1, failure_threshold=1)
    fake.kill("n1")
    cache.check_health()
    assert cache.healthy_nodes == {"n1"}


def test_ttl_survives_rebalancing(cache, fake):
    cache.set("session", "s", ttl=60)
    fake.servers["n4"] = fakeredis.FakeServer()
    fake.clients["n4"] = fake.client("n4")
    cache.add_node("n4", fake.clients["n4"])
    cache.remove_node("n1")
    for n in cache.replica_nodes("session"):
        assert 0 < fake.clients[n].pttl("dc:session") <= 60_000


def test_background_health_checker(cache, fake):
    load(cache)
    cache.start_health_checker(interval=0.05)
    try:
        fake.kill("n2")
        deadline = time.time() + 3
        while "n2" not in cache.down_nodes and time.time() < deadline:
            time.sleep(0.05)
        assert cache.down_nodes == {"n2"}
    finally:
        cache.stop_health_checker()


def test_non_rebalancing_client_only_reroutes(fake):
    worker = DistributedCache(fake.clients, replicas=2, failure_threshold=1, auto_rebalance=False)
    load(worker)
    copies_before = sum(len(fake.keys_on(n)) for n in fake.servers)
    fake.kill("n1")
    assert worker.check_health() is None        # no data moved
    assert worker.down_nodes == {"n1"}
    for k in KEYS:
        assert worker.get(k) == f"v-{k}"         # still served from replicas
    fake.revive("n1")
    worker.check_health()
    assert "n1" in worker.healthy_nodes
    assert sum(len(fake.keys_on(n)) for n in fake.servers) == copies_before  # nothing wiped
