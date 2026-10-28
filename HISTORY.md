# History

This file separates the original project from the rebuild. Dates for the original are from memory; the rebuild's dates are the commit history.

## Original project (2021–2022, college)

| When | Milestone |
| --- | --- |
| Oct 2021 | Started as an LRU cache |
| _to fill in_ | Grew into a distributed cache over Redis nodes with consistent hashing |
| _to fill in_ | Node failure handling and redistribution |
| _to fill in_ | Deployed on AWS; benchmarked read latency under concurrent load |
| Sep 2022 | Finished; documented replication and consistency trade-offs |

The original repository was deleted in 2026 after my GitHub account was compromised, and no copies survived.

## Rebuild (2026)

Rebuilt from scratch following the same path, one commit per stage: LRU cache, hash ring, replication and failover, failure handling and rebalancing, then benchmarks. The benchmarks exposed two problems that are now fixed and documented in the README: library retries that delayed failover by about 5 seconds, and every client repeating the rebalancing work.
