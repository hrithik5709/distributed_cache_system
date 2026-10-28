import threading

import pytest

from dcache.lru import LRUCache


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_get_and_put():
    c = LRUCache(2)
    c.put("a", 1)
    assert c.get("a") == 1
    assert c.get("missing") is None
    assert c.get("missing", "dflt") == "dflt"


def test_evicts_least_recently_used():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    c.get("a")                 # "a" is now more recent than "b"
    c.put("c", 3)
    assert "b" not in c
    assert c.get("a") == 1 and c.get("c") == 3
    assert c.evictions == 1


def test_update_moves_to_front_without_growing():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("a", 10)             # update, not insert
    c.put("c", 3)
    assert len(c) == 2
    assert c.get("a") == 10 and "b" not in c


def test_ttl_expiry():
    clock = FakeClock()
    c = LRUCache(10, default_ttl=5, clock=clock)
    c.put("a", 1)
    c.put("b", 2, ttl=100)
    clock.now = 5
    assert c.get("a") is None  # expired exactly at its deadline
    assert c.get("b") == 2


def test_delete_and_clear():
    c = LRUCache(3)
    c.put("a", 1)
    assert c.delete("a") and not c.delete("a")
    c.put("b", 2)
    c.clear()
    assert len(c) == 0 and c.get("b") is None
    c.put("x", 1)              # list still usable after clear
    assert c.get("x") == 1


def test_hit_and_miss_counters():
    c = LRUCache(2)
    c.put("a", 1)
    c.get("a")
    c.get("zzz")
    assert (c.hits, c.misses) == (1, 1)


def test_rejects_non_positive_capacity():
    with pytest.raises(ValueError):
        LRUCache(0)


def test_thread_safety_keeps_capacity():
    c = LRUCache(100)

    def worker(start):
        for i in range(start, start + 2000):
            c.put(i, i)
            c.get(i - 50)

    threads = [threading.Thread(target=worker, args=(n * 2000,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(c) == 100
    # Walk the linked list to make sure it is still consistent.
    count, node = 0, c._head.next
    while node is not c._tail:
        count += 1
        node = node.next
    assert count == 100
