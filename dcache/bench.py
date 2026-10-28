"""
Read-latency benchmark against real Redis nodes.

    python -m dcache.bench                          # 1, 4, 8 and 16 clients
    python -m dcache.bench --kill n2 --clients 8 --duration 8 --kill-at 3

Each client is a separate process with its own cache client, like separate
application servers. (Python threads do not work for this: each read needs
~0.2 ms of Python CPU time, so threads queue on the GIL and adding them
lowers throughput.) Every client reads random preloaded keys in a loop for
`duration` seconds and records the latency of every call.

The baseline is one Redis node holding all the keys, read with the same
load. With --kill, one node is shut down mid-run and a timeline shows the
effect on latency and hit rate while the clients fail over.
"""

import argparse
import multiprocessing
import random
import time

import redis

from dcache.cluster import DistributedCache, node_client
from dcache.lru import LRUCache

SINGLE_PREFIX = "single:"


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    i = min(len(sorted_values) - 1, max(0, round(p / 100 * len(sorted_values)) - 1))
    return sorted_values[i]


def make_clients(addresses):
    clients = {}
    for i, addr in enumerate(addresses, 1):
        host, port = addr.split(":")
        clients[f"n{i}"] = node_client(host, int(port))
    return clients


def make_cache(args, auto_rebalance=True):
    near = LRUCache(args.near_cache, default_ttl=1.0) if args.near_cache else None
    return DistributedCache(make_clients(args.nodes), replicas=args.replicas,
                            near_cache=near, failure_threshold=3,
                            auto_rebalance=auto_rebalance)


def client_process(job):
    """
    One benchmark client. Returns [(start offset s, latency s, hit), ...].
    time.monotonic() is system-wide on Linux, so offsets from all processes
    share one timeline.
    """
    mode, args, start_at, seed = job
    rng = random.Random(seed)
    if mode == "single":
        node = make_clients(args.nodes[:1])["n1"]
        get = node.get
        keys = [f"{SINGLE_PREFIX}bench:{i}" for i in range(args.keys)]
    else:
        # Only client 0 moves data; the others just stop routing to the dead node.
        cache = make_cache(args, auto_rebalance=(seed == 0))
        if mode == "failover":
            cache.start_health_checker(interval=0.5)
        get = cache.get
        keys = [f"bench:{i}" for i in range(args.keys)]

    while time.monotonic() < start_at:
        time.sleep(0.001)
    deadline = start_at + args.duration
    samples = []
    while True:
        t0 = time.monotonic()
        if t0 >= deadline:
            break
        value = get(keys[rng.randrange(len(keys))])
        samples.append((t0 - start_at, time.monotonic() - t0, value is not None))
    return samples


def run(mode, args, clients, on_start=None):
    start_at = time.monotonic() + 1.0          # give every process time to connect
    jobs = [(mode, args, start_at, seed) for seed in range(clients)]
    with multiprocessing.Pool(clients) as pool:
        pending = pool.map_async(client_process, jobs)
        if on_start:
            on_start(start_at)
        results = pending.get()
    return [s for r in results for s in r]


def summarize(samples, duration):
    lat = sorted(s[1] * 1000 for s in samples)
    hits = sum(1 for s in samples if s[2])
    return {
        "ops/s": len(samples) / duration,
        "p50": percentile(lat, 50), "p95": percentile(lat, 95),
        "p99": percentile(lat, 99), "max": lat[-1] if lat else float("nan"),
        "hit%": 100 * hits / len(samples) if samples else 0.0,
    }


def preload(args):
    cache = make_cache(args)
    single = make_clients(args.nodes[:1])["n1"]
    value = "x" * args.value_size
    for i in range(args.keys):
        cache.set(f"bench:{i}", value)
        single.set(f"{SINGLE_PREFIX}bench:{i}", value)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--nodes", default="localhost:7001,localhost:7002,localhost:7003")
    ap.add_argument("--keys", type=int, default=10_000)
    ap.add_argument("--value-size", type=int, default=100)
    ap.add_argument("--clients", type=int, nargs="+", default=[1, 4, 8, 16],
                    help="number of client processes (one run per value)")
    ap.add_argument("--duration", type=float, default=5.0)
    ap.add_argument("--replicas", type=int, default=2)
    ap.add_argument("--near-cache", type=int, default=0, metavar="SIZE",
                    help="put an LRU near cache of SIZE entries in front (0 = off)")
    ap.add_argument("--kill", metavar="NODE", help="shut this node down mid-run, e.g. n2")
    ap.add_argument("--kill-at", type=float, default=2.0)
    args = ap.parse_args()
    args.nodes = args.nodes.split(",")

    preload(args)
    print(f"{len(args.nodes)} nodes, replicas={args.replicas}, {args.keys} keys of "
          f"{args.value_size} B, {args.duration:.0f} s per run, near cache "
          f"{args.near_cache or 'off'}")

    if args.kill:
        failure_run(args)
        return

    print(f"\n{'':<20}{'clients':>8}{'reads/s':>10}{'p50 ms':>9}{'p95 ms':>9}"
          f"{'p99 ms':>9}{'max ms':>9}{'hit %':>8}")
    for clients in args.clients:
        for mode, label in (("single", "single Redis node"), ("cluster", "distributed cache")):
            s = summarize(run(mode, args, clients), args.duration)
            print(f"{label:<20}{clients:>8}{s['ops/s']:>10.0f}{s['p50']:>9.3f}{s['p95']:>9.3f}"
                  f"{s['p99']:>9.3f}{s['max']:>9.2f}{s['hit%']:>8.1f}")


def failure_run(args):
    clients = args.clients[0]
    victim = make_clients(args.nodes)[args.kill]

    def kill_later(start_at):
        time.sleep(max(0.0, start_at + args.kill_at - time.monotonic()))
        try:
            victim.shutdown(nosave=True)
        except redis.exceptions.ConnectionError:
            pass                       # expected: the server closed the socket

    samples = run("failover", args, clients, on_start=kill_later)

    print(f"\n{clients} clients; {args.kill} shut down at t={args.kill_at:.1f} s\n")
    print(f"{'window':<12}{'reads':>8}{'p50 ms':>9}{'p99 ms':>9}{'max ms':>9}{'hit %':>8}")
    step = 0.5
    for i in range(int(args.duration / step)):
        window = [s for s in samples if i * step <= s[0] < (i + 1) * step]
        if not window:
            continue
        s = summarize(window, step)
        mark = "  <- node killed" if i * step <= args.kill_at < (i + 1) * step else ""
        print(f"{i * step:>4.1f}-{(i + 1) * step:<4.1f}s {len(window):>8}{s['p50']:>9.3f}"
              f"{s['p99']:>9.3f}{s['max']:>9.2f}{s['hit%']:>8.1f}{mark}")

    total = summarize(samples, args.duration)
    print(f"\noverall: {total['ops/s']:.0f} reads/s, hit rate {total['hit%']:.2f}%")
    print("restart the node with: scripts/nodes.sh start")


if __name__ == "__main__":
    main()
