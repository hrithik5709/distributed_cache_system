"""
A distributed cache client over a set of Redis nodes.

Placement: each key lives on `replicas` distinct nodes, chosen by walking a
consistent hash ring clockwise from the key (the first is the primary).

Writes go to every replica. The write succeeds when at least
`write_quorum` replicas acknowledge it; a failed replica is skipped.

Reads try the replicas in ring order and return the first hit. If an
earlier replica answered "miss" while a later one had the value, the value
is copied back (read repair), so replicas converge after a failure.

Node errors never surface as exceptions on reads: a cache that cannot reach
a node behaves like a cache miss. Errors are counted per node, and the
health checker uses them to take nodes out of the ring.
"""

import threading

import redis

from dcache.hash_ring import HashRing

NODE_ERRORS = (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)


class CacheUnavailable(Exception):
    """Fewer than write_quorum replicas accepted a write."""


class DistributedCache:
    def __init__(self, nodes, replicas=2, vnodes=100, write_quorum=1,
                 failure_threshold=3, near_cache=None, key_prefix="dc:"):
        """
        nodes: {name: redis client}. Clients should use decode_responses=True.
        near_cache: optional LRUCache in front of the cluster (see docs).
        """
        if not 1 <= write_quorum <= replicas:
            raise ValueError("write_quorum must be between 1 and replicas")
        self.replicas = replicas
        self.write_quorum = write_quorum
        self.failure_threshold = failure_threshold
        self.near_cache = near_cache
        self.key_prefix = key_prefix
        self._clients = dict(nodes)
        self._failures = {name: 0 for name in nodes}
        self._down = set()
        self._lock = threading.Lock()
        self.ring = HashRing(nodes, vnodes=vnodes)
        self.stats = {"hits": 0, "misses": 0, "near_hits": 0,
                      "node_errors": 0, "read_repairs": 0}

    # ---- public API ---------------------------------------------------

    def get(self, key, default=None):
        if self.near_cache is not None:
            value = self.near_cache.get(key)
            if value is not None:
                self._count("near_hits")
                return value

        rkey = self._rkey(key)
        missed = []                    # replicas that answered "not found"
        for node in self.replica_nodes(key):
            try:
                value = self._clients[node].get(rkey)
            except NODE_ERRORS:
                self._record_failure(node)
                continue
            self._record_success(node)
            if value is None:
                missed.append(node)
                continue
            if missed:
                self._read_repair(rkey, node, missed)
            if self.near_cache is not None:
                self.near_cache.put(key, value)
            self._count("hits")
            return value

        self._count("misses")
        return default

    def set(self, key, value, ttl=None):
        """
        ttl in seconds (float allowed). Returns the number of replicas written.
        Raises CacheUnavailable if fewer than write_quorum accepted it.
        """
        rkey = self._rkey(key)
        px = int(ttl * 1000) if ttl is not None else None
        acks = 0
        for node in self.replica_nodes(key):
            try:
                self._clients[node].set(rkey, value, px=px)
            except NODE_ERRORS:
                self._record_failure(node)
                continue
            self._record_success(node)
            acks += 1
        if self.near_cache is not None:
            self.near_cache.delete(key)
        if acks < self.write_quorum:
            raise CacheUnavailable(f"{acks} of {self.write_quorum} required replicas wrote {key!r}")
        return acks

    def delete(self, key):
        rkey = self._rkey(key)
        deleted = False
        for node in self.replica_nodes(key):
            try:
                deleted = bool(self._clients[node].delete(rkey)) or deleted
            except NODE_ERRORS:
                self._record_failure(node)
        if self.near_cache is not None:
            self.near_cache.delete(key)
        return deleted

    def replica_nodes(self, key):
        """Healthy nodes holding `key`, primary first."""
        return self.ring.get_nodes(key, self.replicas)

    @property
    def healthy_nodes(self):
        return self.ring.nodes

    @property
    def down_nodes(self):
        return set(self._down)

    # ---- internals ----------------------------------------------------

    def _rkey(self, key):
        return f"{self.key_prefix}{key}"

    def _read_repair(self, rkey, source, targets):
        try:
            pttl = self._clients[source].pttl(rkey)
        except NODE_ERRORS:
            return
        value = self._clients[source].get(rkey)
        if value is None:
            return
        px = pttl if pttl and pttl > 0 else None
        for node in targets:
            try:
                # nx: never overwrite a value written since we looked.
                self._clients[node].set(rkey, value, px=px, nx=True)
                self._count("read_repairs")
            except NODE_ERRORS:
                self._record_failure(node)

    def _record_failure(self, node):
        with self._lock:
            self._failures[node] = self._failures.get(node, 0) + 1
            self.stats["node_errors"] += 1

    def _record_success(self, node):
        with self._lock:
            self._failures[node] = 0

    def _count(self, name):
        with self._lock:
            self.stats[name] += 1
