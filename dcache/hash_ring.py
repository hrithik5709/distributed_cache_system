"""
Consistent hashing with virtual nodes.

Every physical node is placed on a ring of 64-bit positions many times
("virtual nodes"). A key belongs to the first virtual node clockwise from
the key's own position. Adding or removing a node only changes ownership of
the ranges next to its virtual nodes, so roughly 1/N of the keys move,
instead of nearly all of them with `hash(key) % N`.

Positions come from MD5 rather than Python's hash(), which is randomised per
process: every client must compute the same ring.
"""

import bisect
import hashlib
import threading


def _position(text):
    return int.from_bytes(hashlib.md5(text.encode()).digest()[:8], "big")


class HashRing:
    def __init__(self, nodes=(), vnodes=100):
        if vnodes <= 0:
            raise ValueError("vnodes must be positive")
        self.vnodes = vnodes
        self._positions = []           # sorted virtual-node positions
        self._owners = {}              # position -> physical node name
        self._nodes = set()
        self._lock = threading.Lock()
        for node in nodes:
            self.add_node(node)

    @property
    def nodes(self):
        return set(self._nodes)

    def add_node(self, node):
        with self._lock:
            if node in self._nodes:
                return
            self._nodes.add(node)
            for i in range(self.vnodes):
                pos = _position(f"{node}#{i}")
                if pos in self._owners:
                    continue           # astronomically rare 64-bit collision
                self._owners[pos] = node
                bisect.insort(self._positions, pos)

    def remove_node(self, node):
        with self._lock:
            if node not in self._nodes:
                return
            self._nodes.discard(node)
            self._positions = [p for p in self._positions if self._owners[p] != node]
            self._owners = {p: n for p, n in self._owners.items() if n != node}

    def get_node(self, key):
        nodes = self.get_nodes(key, 1)
        return nodes[0]

    def get_nodes(self, key, count):
        """
        The first `count` distinct physical nodes clockwise from the key:
        the key's primary followed by its replicas.
        """
        with self._lock:
            if not self._positions:
                raise LookupError("hash ring is empty")
            count = min(count, len(self._nodes))
            start = bisect.bisect(self._positions, _position(str(key)))
            result = []
            for i in range(len(self._positions)):
                pos = self._positions[(start + i) % len(self._positions)]
                owner = self._owners[pos]
                if owner not in result:
                    result.append(owner)
                    if len(result) == count:
                        break
            return result

    def __len__(self):
        return len(self._nodes)
