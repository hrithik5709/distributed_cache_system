import hashlib
from collections import Counter

import pytest

from dcache.hash_ring import HashRing

KEYS = [f"user:{i}" for i in range(20000)]


def test_same_key_always_maps_to_same_node():
    a = HashRing(["n1", "n2", "n3"])
    b = HashRing(["n3", "n1", "n2"])          # insertion order must not matter
    assert all(a.get_node(k) == b.get_node(k) for k in KEYS[:1000])


def test_keys_spread_evenly_with_virtual_nodes():
    ring = HashRing(["n1", "n2", "n3", "n4"], vnodes=200)
    counts = Counter(ring.get_node(k) for k in KEYS)
    expected = len(KEYS) / 4
    for n in counts.values():
        assert abs(n - expected) / expected < 0.15


def test_adding_a_node_moves_about_one_nth_of_keys():
    ring = HashRing(["n1", "n2", "n3"], vnodes=200)
    before = {k: ring.get_node(k) for k in KEYS}
    ring.add_node("n4")
    moved = [k for k in KEYS if ring.get_node(k) != before[k]]
    assert 0.18 < len(moved) / len(KEYS) < 0.32         # ideal: 1/4
    assert all(ring.get_node(k) == "n4" for k in moved)  # only to the new node


def test_modulo_hashing_moves_most_keys_for_comparison():
    def mod_node(key, n):
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % n

    moved = sum(mod_node(k, 3) != mod_node(k, 4) for k in KEYS)
    assert moved / len(KEYS) > 0.7                      # ideal: 3/4


def test_removing_a_node_only_moves_its_keys():
    ring = HashRing(["n1", "n2", "n3", "n4"])
    before = {k: ring.get_node(k) for k in KEYS}
    ring.remove_node("n2")
    for k in KEYS:
        if before[k] != "n2":
            assert ring.get_node(k) == before[k]
        else:
            assert ring.get_node(k) != "n2"


def test_get_nodes_returns_distinct_replicas_in_ring_order():
    ring = HashRing(["n1", "n2", "n3"])
    for k in KEYS[:500]:
        replicas = ring.get_nodes(k, 2)
        assert len(set(replicas)) == 2
        assert replicas[0] == ring.get_node(k)
    assert len(ring.get_nodes("x", 10)) == 3            # capped at node count


def test_removing_primary_promotes_first_replica():
    ring = HashRing(["n1", "n2", "n3"])
    for k in KEYS[:500]:
        primary, replica = ring.get_nodes(k, 2)
        smaller = HashRing(["n1", "n2", "n3"])
        smaller.remove_node(primary)
        assert smaller.get_node(k) == replica


def test_empty_ring_and_duplicates():
    ring = HashRing()
    with pytest.raises(LookupError):
        ring.get_node("x")
    ring.add_node("n1")
    ring.add_node("n1")
    assert len(ring) == 1 and len(ring._positions) == 100
    ring.remove_node("missing")                          # no error
