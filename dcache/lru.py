"""
An in-process LRU cache with optional per-key TTL.

A hash map gives O(1) lookup; a doubly linked list keeps entries in recency
order so the least recently used one can be evicted in O(1). The list has
sentinel head and tail nodes, so insertion and removal never special-case
an empty list.
"""

import threading
import time


class _Entry:
    __slots__ = ("key", "value", "expires_at", "prev", "next")

    def __init__(self, key=None, value=None, expires_at=None):
        self.key = key
        self.value = value
        self.expires_at = expires_at
        self.prev = None
        self.next = None


class LRUCache:
    def __init__(self, capacity, default_ttl=None, clock=time.monotonic):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.default_ttl = default_ttl
        self._clock = clock            # injectable so tests control time
        self._map = {}
        self._lock = threading.Lock()
        # Sentinels: head.next is the most recent entry, tail.prev the least.
        self._head = _Entry()
        self._tail = _Entry()
        self._head.next = self._tail
        self._tail.prev = self._head
        self.hits = self.misses = self.evictions = 0

    def get(self, key, default=None):
        with self._lock:
            entry = self._map.get(key)
            if entry is None:
                self.misses += 1
                return default
            if self._expired(entry):
                # Lazy expiry: expired entries are removed when touched.
                self._remove(entry)
                del self._map[key]
                self.misses += 1
                return default
            self._move_to_front(entry)
            self.hits += 1
            return entry.value

    def put(self, key, value, ttl=None):
        ttl = self.default_ttl if ttl is None else ttl
        expires_at = self._clock() + ttl if ttl is not None else None
        with self._lock:
            entry = self._map.get(key)
            if entry is not None:
                entry.value = value
                entry.expires_at = expires_at
                self._move_to_front(entry)
                return
            entry = _Entry(key, value, expires_at)
            self._map[key] = entry
            self._add_front(entry)
            if len(self._map) > self.capacity:
                lru = self._tail.prev
                self._remove(lru)
                del self._map[lru.key]
                self.evictions += 1

    def delete(self, key):
        with self._lock:
            entry = self._map.pop(key, None)
            if entry is None:
                return False
            self._remove(entry)
            return True

    def clear(self):
        with self._lock:
            self._map.clear()
            self._head.next = self._tail
            self._tail.prev = self._head

    def __len__(self):
        return len(self._map)

    def __contains__(self, key):
        return self.get(key, _MISSING) is not _MISSING

    def _expired(self, entry):
        return entry.expires_at is not None and self._clock() >= entry.expires_at

    def _add_front(self, entry):
        entry.prev = self._head
        entry.next = self._head.next
        self._head.next.prev = entry
        self._head.next = entry

    def _remove(self, entry):
        entry.prev.next = entry.next
        entry.next.prev = entry.prev

    def _move_to_front(self, entry):
        self._remove(entry)
        self._add_front(entry)


_MISSING = object()
