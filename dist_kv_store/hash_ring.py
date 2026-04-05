import bisect
import hashlib


def hash_str(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    def __init__(self, nodes: list[str], virtual_nodes: int = 5) -> None:
        if not nodes:
            raise ValueError("nodes must not be empty")
        if virtual_nodes <= 0:
            raise ValueError("virtual_nodes must be positive")

        self.nodes = list(nodes)
        self.virtual_nodes = virtual_nodes
        self.ring: list[int] = []
        self.node_map: dict[int, str] = {}

        for node in self.nodes:
            for i in range(self.virtual_nodes):
                vnode_key = f"{node}#{i}"
                h = hash_str(vnode_key)
                self.ring.append(h)
                self.node_map[h] = node

        self.ring.sort()

    def get_node(self, key: str) -> str:
        h = hash_str(key)
        idx = bisect.bisect(self.ring, h)

        if idx == len(self.ring):
            idx = 0

        return self.node_map[self.ring[idx]]

    def describe_ring(self) -> list[dict]:
        return [
            {"hash": h, "node": self.node_map[h]}
            for h in self.ring
        ]