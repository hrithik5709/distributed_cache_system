# Distributed Cache

A Python client that turns several independent Redis servers into one fault-tolerant cache. Keys are spread with consistent hashing and replicated to two nodes. When a node dies, reads fail over to a replica within milliseconds, and its data is re-replicated. When nodes are added or removed, only the keys whose owners changed are moved.

> **About this repository.** I originally built this project in 2021–2022 in college: it started as an LRU cache and grew into a distributed cache. The original repository was lost in 2026 when my GitHub account was compromised, so this is a ground-up rebuild of the same design, with real benchmarks and the lessons from them. See [HISTORY.md](HISTORY.md).

## Features

- **Consistent hashing** with virtual nodes (`dcache/hash_ring.py`). Adding a 4th node to 3 moves 26.8% of keys; modulo hashing would move 74.9%.
- **Replication**: each key is stored on 2 nodes (configurable), with a configurable write quorum.
- **Failover and read repair**: reads fall back to the next replica; a replica found missing a key is refilled.
- **Failure detection**: a node is taken out of the ring after 3 consecutive errors, from traffic or a background health check.
- **Rebalancing**: on node failure, recovery, addition or planned removal, keys are copied to their new owners, with TTLs preserved and nothing deleted until every owner has a copy.
- **Near cache**: an optional O(1) LRU with TTL inside each client (`dcache/lru.py`).
- **Benchmark** of read latency under concurrent load, including a node being killed mid-run (`dcache/bench.py`).

## Quick start

Needs Python 3.10+ and Redis.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
scripts/nodes.sh start                 # three Redis nodes on ports 7001-7003
.venv/bin/python -m pytest             # 39 tests, no Redis needed (uses fakeredis)
.venv/bin/python -m dcache.bench       # latency benchmark against the real nodes
.venv/bin/python -m dcache.bench --kill n2 --clients 8 --duration 8 --kill-at 3
```

`docker-compose.yml` starts the same three nodes in containers. (It is valid, but was not run on the machine that produced the numbers below.)

```python
from dcache.cluster import DistributedCache, node_client
from dcache.lru import LRUCache

nodes = {f"n{i}": node_client("localhost", 7000 + i) for i in (1, 2, 3)}
cache = DistributedCache(nodes, replicas=2, near_cache=LRUCache(10_000, default_ttl=1.0))
cache.start_health_checker(interval=0.5)

cache.set("user:42", "alice", ttl=300)
cache.get("user:42")              # 'alice'
cache.replica_nodes("user:42")    # ['n1', 'n2']
```

## Design

```
application ── DistributedCache ──┬── near cache (LRU, optional)
                                  ├── HashRing: key -> [primary, replica]
                                  ├── health checker: ping + error counts
                                  └── node clients ── Redis n1 / n2 / n3
```

| Operation | What happens |
| --- | --- |
| `set` | Write to every replica of the key; succeed if `write_quorum` replicas acknowledged |
| `get` | Try replicas in ring order; return the first hit; refill replicas that missed |
| Node fails | After 3 errors it leaves the ring, so its keys route to the next nodes; one client re-replicates |
| Node recovers | Its stale keys are wiped (it missed writes and deletes), it rejoins, and receives its keys |
| `add_node` / `remove_node` | Ring updated; only keys whose owners changed are copied and removed |

The full reasoning, including what each choice gives up, is in **[docs/TRADEOFFS.md](docs/TRADEOFFS.md)**.

## Benchmark results

Machine: 8-core laptop under WSL2, Redis 8.0.5, Python 3.14, three nodes on localhost. 10,000 keys of 100 bytes, uniform random reads, 5 s per run. Each client is a separate process with its own cache client.

| Clients | Single Redis node: reads/s | p50 / p99 ms | Distributed cache: reads/s | p50 / p99 ms |
| --- | --- | --- | --- | --- |
| 1 | 1,791 | 0.48 / 1.28 | 1,651 | 0.51 / 1.40 |
| 4 | 7,516 | 0.50 / 1.28 | 5,619 | 0.66 / 1.54 |
| 8 | 10,302 | 0.71 / 1.95 | 8,843 | 0.77 / 2.59 |
| 16 | 14,852 | 0.93 / 3.69 | 11,966 | 1.16 / 4.28 |

**How to read this.** On one machine the clients' CPU is the bottleneck, not Redis, so the cluster is not faster than one node. Across runs it was sometimes above it and sometimes below. Its benefits are capacity, failure tolerance and scaling across machines, which a single-machine run cannot show. The ring lookup costs about 5 µs; the rest of the gap is mostly run-to-run noise (single-node p50 alone varied from 0.46 to 0.68 ms between identical runs).

**Node killed mid-run** (8 clients, n2 shut down at t = 3 s): 100% hit rate throughout, and the worst read in the half-second of the failure was 7–9 ms across three runs. Throughput showed no sustained drop.

**Near cache** (8 clients, 1,000 hot keys): 116,456 reads/s at p50 0.005 ms, against 8,555 reads/s at p50 0.775 ms without it. The cost is up to 1 s of staleness for other clients' writes.

## What the benchmarks taught me

1. **Python threads cannot load-test this.** Each read needs about 0.2 ms of Python CPU time, so threads queue on the GIL: throughput *fell* from 2,263/s with 1 thread to 1,062/s with 32. Separate processes scale (1,745/s to 12,859/s at 16), so the benchmark uses processes, like separate app servers.
2. **Library retries hid a 5-second failover.** The first failure test froze reads for about 4.9 s each. redis-py 8 retries a failed command 10 times with backoff (3.6 s against a dead node). The cache already fails over to a replica, so `node_client()` turns library retries off. Worst read during a failure: 4.9 s, now 7–9 ms.
3. **Only one client should move data.** When every client rebalanced after a failure, 8 processes repeated the same copy work and throughput kept dipping. `auto_rebalance` is now enabled on one client; the others just stop routing to the dead node.

## Project layout

```
dcache/lru.py          O(1) LRU cache with TTL (hash map + doubly linked list)
dcache/hash_ring.py    consistent hash ring with virtual nodes
dcache/cluster.py      DistributedCache: replication, failover, health checks, rebalancing
dcache/bench.py        multi-process latency benchmark, with optional node kill
scripts/nodes.sh       start / stop / status of three local Redis nodes
docs/TRADEOFFS.md      replication and consistency trade-offs
tests/                 39 tests; fakeredis simulates nodes going down
```

## Limitations and next steps

- No read quorum or write ordering across clients: replicas can briefly disagree (see TRADEOFFS.md, section 3).
- Rebalancing scans every key; moving only the changed ring arcs would be O(moved keys).
- Membership is decided by each client; a coordinator with a membership epoch would remove disagreements.
- Near-cache invalidation is local only; Redis client-side caching (`CLIENT TRACKING`) could push invalidations.
- Not yet deployed for this rebuild: the original ran on AWS; redeploying across availability zones is next.
