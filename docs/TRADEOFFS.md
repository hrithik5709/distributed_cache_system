# Replication and consistency trade-offs

This cache chooses **availability and low latency over strong consistency**. That is the right default for a cache: the source of truth is a database, and a stale or missing entry costs a slower request, not a wrong answer stored forever. Each section below states what the code does, what it gives up, and the alternative.

## 1. Placement: consistent hashing with virtual nodes

**Choice.** Each node is hashed onto a 64-bit ring 100 times. A key belongs to the first node clockwise from its hash; its replicas are the next distinct nodes.

**Why.** With `hash(key) % N`, changing N remaps almost every key. Measured on 20,000 keys, going from 3 to 4 nodes moves **74.9%** of keys with modulo hashing and **26.8%** with the ring (ideal: 25%). Virtual nodes keep the load even: with 1 position per node, a node can own a much larger arc of the ring than the others.

**Cost.** Every client must compute the same ring, so positions use MD5, not Python's per-process randomised `hash()`. More virtual nodes give a smoother spread but a bigger ring: 100 per node is about 3 KB of positions per node.

**Alternatives.** Rendezvous (highest-random-weight) hashing needs no ring but costs O(N) per lookup. Redis Cluster uses 16,384 fixed hash slots assigned to nodes, which makes moving data an explicit slot-by-slot operation.

## 2. Replication factor and write quorum

**Choice.** `replicas=2`: each key is written to 2 nodes. `write_quorum=1` by default: a write succeeds if at least one replica took it.

| Setting | Survives | Write latency | Risk |
| --- | --- | --- | --- |
| replicas=1 | Nothing: a node loss loses its keys (they become misses) | Lowest | Miss storm on the database after a failure |
| replicas=2, W=1 (default) | One node failure without misses | Sequential writes to 2 nodes | A write that reached only one replica can be lost if that node then dies |
| replicas=2, W=2 | Same | Same, but fails when any replica is down | Writes fail during a node outage |
| replicas=3, W=2 | Two failures | Higher | 50% more memory |

**Why W=1.** For a cache, a lost write means a later miss and a database read, which is cheap. Rejecting writes during an outage would add errors for no real benefit.

**Not implemented:** read quorums (R + W > N). Reads take the first replica that has the key, so two replicas can briefly disagree (next section). A system needing linearizable reads would use R + W > N or a consensus protocol (Raft), at a large latency cost.

## 3. Consistency: what a reader can observe

The cache is **eventually consistent**. Concretely:

- **Concurrent writers.** Two clients writing the same key may reach the two replicas in different orders, leaving them with different values. Nothing orders writes across clients. Last-writer-wins with timestamps or version numbers would fix it but needs either synchronised clocks or a read before every write.
- **Partial writes.** With W=1, a write can land on the replica but fail on the primary. Readers still see the old value from the primary until it is overwritten or expires. **Read repair** only fixes a replica that is *missing* the key, not one holding a different value, so TTLs are the backstop: every entry should have one.
- **Failover reads.** When the primary dies, reads move to the replica. If the last write to that key only reached the primary, readers see the previous value. This is the stale-read window that W=2 would close.
- **Recovered nodes.** A node that returns missed writes and deletes while it was down, so the rebalancing client wipes its keys before it rejoins. Without the wipe, a deleted key could come back. A test proves this: disabling the wipe makes `test_recovered_node_is_wiped_and_rejoins` fail.

## 4. Failure detection

**Choice.** A node is marked down after `failure_threshold` (3) consecutive errors, counted from both regular traffic and a background `PING` every 0.5 s. A down node is removed from the ring at once, so reads go straight to the next replica.

**Trade-off.** A low threshold reacts fast but flaps on a network blip; a high one keeps sending requests to a dead node. Each client decides on its own, so two clients can briefly disagree about membership. That is acceptable for a cache, because both still find the data on a replica.

**Lesson from the benchmark.** The first failover test froze reads for **about 4.9 s** per request: redis-py 8 retries a failed command 10 times with exponential backoff (3.6 s against a dead node) before raising. The cache was already designed to fail over, so the library's retries just delayed it. With library retries off (`node_client()`), the worst read during a node kill was **7–9 ms** across three runs. Retrying at more than one layer is a classic way to turn a failure into an outage.

## 5. Rebalancing

**Choice.** After a membership change, one client scans the keys on each node, copies each key to any owner missing it (`SET NX`, preserving TTL), and deletes it from nodes that are no longer owners. A key is deleted only after every owner has it, so an unreachable owner never causes data loss.

**Trade-offs.**

- **Only one client should rebalance** (`auto_rebalance=True` on one process). In the first version every client process rebalanced after a failure, repeating the same copy work 8 times; throughput kept dipping for seconds afterwards. With one rebalancer, the failure barely shows in the latency timeline.
- **Scanning every key** is O(total keys) per change. Production systems move only the affected ranges (Redis Cluster moves hash slots). The ring already knows which arcs changed, so that is the natural next step.
- **Races.** A write during rebalancing can land on a node that is about to drop the key. `SET NX` guarantees an older copy never overwrites a newer value, but there is no global ordering. A coordinator with versioned membership (an "epoch") would close this.

## 6. Near cache (in-process LRU)

**Choice.** Optional: an `LRUCache` inside each client with a short TTL (1 s in the benchmark). A client's own writes and deletes invalidate its near cache.

**Measured.** 8 clients reading 1,000 hot keys: **116,456 reads/s, p50 0.005 ms** with the near cache, against **8,555 reads/s, p50 0.775 ms** without it.

**Cost.** Writes by *other* clients are invisible until the entry's TTL expires: up to 1 s of staleness. Fixing that needs invalidation messages (for example Redis pub/sub or client-side caching with `CLIENT TRACKING`), which add a dependency and a failure mode. Use the near cache only for data where a second of staleness is fine.

## 7. What the benchmark does and does not show

On one 8-core machine (WSL2, Redis 8.0.5, Python 3.14), client CPU is the bottleneck, not Redis. Across runs the cluster's throughput was in the same range as a single Redis node, sometimes above and sometimes below. The cluster's advantages are **capacity** (data larger than one node's memory), **failure tolerance** (no misses when a node dies) and **horizontal scaling** across machines. A single-machine benchmark measures none of those well. The routing overhead itself is small: about 5 µs for the ring lookup, and tens of microseconds per request in total under the profiler, which is below the run-to-run noise.
