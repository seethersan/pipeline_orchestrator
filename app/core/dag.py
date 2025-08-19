
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

class CycleError(Exception):
    def __init__(self, cycle_path: List[int]):
        self.cycle_path = cycle_path
        super().__init__(f"DAG has a cycle: {' -> '.join(map(str, cycle_path))}")

@dataclass(frozen=True)
class Graph:
    nodes: Set[int]
    edges: List[Tuple[int, int]]
    adj: Dict[int, Set[int]]
    indegree: Dict[int, int]

def build_graph(block_ids: List[int], edges: List[Tuple[int, int]]) -> Graph:
    nodes = set(block_ids)
    adj = {b: set() for b in block_ids}
    indegree = {b: 0 for b in block_ids}
    for u, v in edges:
        if u not in nodes or v not in nodes:
            raise ValueError(f"Edge references unknown node: ({u}, {v})")
        if u == v:
            raise CycleError([u, v])
        if v not in adj[u]:
            adj[u].add(v)
            indegree[v] += 1
    return Graph(nodes=nodes, edges=edges, adj=adj, indegree=indegree)

def topological_sort(block_ids: List[int], edges: List[Tuple[int, int]]) -> List[int]:
    g = build_graph(block_ids, edges)
    queue = [n for n, d in g.indegree.items() if d == 0]
    order: List[int] = []
    indeg = dict(g.indegree)
    adj = g.adj
    while queue:
        n = queue.pop(0)
        order.append(n)
        for v in list(adj[n]):
            indeg[v] -= 1
            if indeg[v] == 0:
                queue.append(v)
    if len(order) != len(g.nodes):
        visited: Set[int] = set()
        stack: Set[int] = set()
        path: List[int] = []
        def dfs(u: int) -> bool:
            visited.add(u)
            stack.add(u)
            path.append(u)
            for v in adj[u]:
                if v not in visited:
                    if dfs(v):
                        return True
                elif v in stack:
                    path.append(v)
                    return True
            stack.remove(u)
            path.pop()
            return False
        for start in g.nodes:
            if start not in visited:
                if dfs(start):
                    last = path[-1]
                    cycle = [last]
                    for x in reversed(path[:-1]):
                        cycle.append(x)
                        if x == last:
                            break
                    cycle.reverse()
                    raise CycleError(cycle)
        raise CycleError([])
    return order

def find_roots(block_ids: List[int], edges: List[Tuple[int, int]]) -> List[int]:
    g = build_graph(block_ids, edges)
    return [n for n, d in g.indegree.items() if d == 0]

def next_runnables(block_ids: List[int], edges: List[Tuple[int, int]], completed: Set[int], running: Set[int] | None = None) -> Set[int]:
    g = build_graph(block_ids, edges)
    running = running or set()
    preds: Dict[int, Set[int]] = {n: set() for n in block_ids}
    for u in block_ids:
        for v in g.adj[u]:
            preds[v].add(u)
    out: Set[int] = set()
    for n in block_ids:
        if n in completed or n in running:
            continue
        if all(p in completed for p in preds[n]):
            out.add(n)
    return out
