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
a node behaves like a cache miss. After `failure_threshold` consecutive
errors a node is marked down and removed from the ring, so its key ranges
move to the next nodes clockwise. `rebalance()` then copies keys until every
key is on its full set of replicas again. A node that comes back is wiped
before rejoining, because it missed writes and deletes while it was away.
"""

import threading

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from dcache.hash_ring import HashRing

NODE_ERRORS = (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)


def node_client(host="localhost", port=6379, timeout=0.5, **kwargs):
    """
    A Redis client suitable for a cache node.

    redis-py retries failed commands itself (redis-py 8: 10 retries with
    exponential backoff, about 3.6 s against a dead node). This cache
    already fails over to another replica, so retrying inside the client
    only delays failover. Retries are turned off and timeouts kept short.
    """
    return redis.Redis(host=host, port=port, decode_responses=True,
                       socket_timeout=timeout, socket_connect_timeout=timeout,
                       retry=Retry(NoBackoff(), 0), **kwargs)


class CacheUnavailable(Exception):
    """Fewer than write_quorum replicas accepted a write."""


class DistributedCache:
    def __init__(self, nodes, replicas=2, vnodes=100, write_quorum=1,
                 failure_threshold=3, near_cache=None, key_prefix="dc:",
                 auto_rebalance=True):
        """
        nodes: {name: redis client}. Clients should use decode_responses=True;
            node_client() builds suitable ones.
        near_cache: optional LRUCache in front of the cluster (see docs).
        auto_rebalance: whether this client moves data when membership
            changes (wiping recovered nodes and re-replicating keys). With
            many application processes, enable it on one of them only: the
            others just stop routing to dead nodes, instead of every process
            repeating the same copy work.
        """
        if not 1 <= write_quorum <= replicas:
            raise ValueError("write_quorum must be between 1 and replicas")
        self.replicas = replicas
        self.write_quorum = write_quorum
        self.failure_threshold = failure_threshold
        self.near_cache = near_cache
        self.key_prefix = key_prefix
        self.auto_rebalance = auto_rebalance
        self._clients = dict(nodes)
        self._failures = {name: 0 for name in nodes}
        self._down = set()
        self._needs_rebalance = False
        self._lock = threading.Lock()
        self._rebalance_lock = threading.Lock()
        self._checker = None
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

    # ---- membership and health ---------------------------------------

    def check_health(self):
        """
        Ping every node once. Down nodes that answer are wiped and rejoin;
        healthy nodes that keep failing are taken out. Rebalances if the
        membership changed. Returns the rebalance stats, or None.
        """
        for node, client in list(self._clients.items()):
            try:
                client.ping()
            except NODE_ERRORS:
                self._record_failure(node)
                continue
            self._record_success(node)
            if node in self._down:
                self._mark_up(node)
        if self._needs_rebalance and self.auto_rebalance:
            return self.rebalance()
        return None

    def start_health_checker(self, interval=1.0):
        """Run check_health() every `interval` seconds in a daemon thread."""
        if self._checker is not None:
            return
        stop = threading.Event()

        def loop():
            while not stop.wait(interval):
                self.check_health()

        thread = threading.Thread(target=loop, name="dcache-health", daemon=True)
        self._checker = (thread, stop)
        thread.start()

    def stop_health_checker(self):
        if self._checker is not None:
            thread, stop = self._checker
            stop.set()
            thread.join()
            self._checker = None

    def add_node(self, name, client):
        """Add a node; it receives the keys it now owns. Returns rebalance stats."""
        with self._lock:
            self._clients[name] = client
            self._failures[name] = 0
        self.ring.add_node(name)
        return self.rebalance()

    def remove_node(self, name):
        """
        Planned removal: the node leaves the ring first, its keys are copied
        to their new owners, then it is forgotten. Returns rebalance stats.
        """
        self.ring.remove_node(name)
        stats = self.rebalance(extra_sources=[name])
        with self._lock:
            self._clients.pop(name, None)
            self._failures.pop(name, None)
            self._down.discard(name)
        return stats

    def rebalance(self, extra_sources=()):
        """
        Make every key live on exactly its current replica set.

        Scans the keys on each node, copies a key to any owner missing it,
        then deletes it from nodes that are no longer owners. A key is only
        deleted once every owner has it, so an unreachable owner never
        causes data loss. Copies use NX so a newer write is never replaced
        by an older copy.
        """
        with self._rebalance_lock:
            self._needs_rebalance = False
            stats = {"scanned": 0, "copied": 0, "removed": 0}
            sources = list(self.ring.nodes) + [n for n in extra_sources if n in self._clients]
            for node in sources:
                client = self._clients[node]
                try:
                    keys = list(client.scan_iter(match=f"{self.key_prefix}*", count=500))
                except NODE_ERRORS:
                    self._record_failure(node)
                    continue
                for rkey in keys:
                    stats["scanned"] += 1
                    self._place_key(node, rkey, stats)
            return stats

    def _place_key(self, source, rkey, stats):
        try:
            client = self._clients[source]
            value = client.get(rkey)
            pttl = client.pttl(rkey)
        except NODE_ERRORS:
            self._record_failure(source)
            return
        if value is None:              # expired or deleted since the scan
            return
        owners = self.ring.get_nodes(rkey[len(self.key_prefix):], self.replicas)
        px = pttl if pttl > 0 else None
        all_owners_have_it = True
        for owner in owners:
            if owner == source:
                continue
            try:
                if self._clients[owner].set(rkey, value, px=px, nx=True):
                    stats["copied"] += 1
            except NODE_ERRORS:
                self._record_failure(owner)
                all_owners_have_it = False
        if source not in owners and all_owners_have_it:
            try:
                client.delete(rkey)
                stats["removed"] += 1
            except NODE_ERRORS:
                self._record_failure(source)

    def _mark_down(self, node):
        # Caller holds self._lock.
        if node in self._down or node not in self.ring.nodes:
            return
        if len(self.ring) == 1:
            return                     # never remove the last node
        self._down.add(node)
        self.ring.remove_node(node)
        self._needs_rebalance = True

    def _mark_up(self, node):
        # Only the rebalancing client wipes the returning node; a late wipe
        # from another process could delete keys already copied back to it.
        if self.auto_rebalance:
            client = self._clients[node]
            try:
                stale = list(client.scan_iter(match=f"{self.key_prefix}*", count=500))
                if stale:
                    client.delete(*stale)
            except NODE_ERRORS:
                self._record_failure(node)
                return
        with self._lock:
            self._down.discard(node)
            self._failures[node] = 0
            self._needs_rebalance = True
        self.ring.add_node(node)

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
            if self._failures[node] >= self.failure_threshold:
                self._mark_down(node)

    def _record_success(self, node):
        with self._lock:
            self._failures[node] = 0

    def _count(self, name):
        with self._lock:
            self.stats[name] += 1
