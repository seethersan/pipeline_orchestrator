import pytest
from app.core.dag import topological_sort, find_roots, next_runnables, CycleError


def test_topological_sort_valid():
    blocks = [1, 2, 3, 4, 5]
    edges = [(1, 2), (1, 3), (2, 4), (3, 5)]
    order = topological_sort(blocks, edges)
    assert order.index(1) < order.index(2)
    assert order.index(1) < order.index(3)
    assert order.index(2) < order.index(4)
    assert order.index(3) < order.index(5)
    assert find_roots(blocks, edges) == [1]


def test_topological_sort_cycle_detected():
    blocks = [1, 2, 3]
    edges = [(1, 2), (2, 3), (3, 1)]
    with pytest.raises(CycleError):
        topological_sort(blocks, edges)


def test_next_runnables():
    blocks = [1, 2, 3, 4, 5]
    edges = [(1, 2), (1, 3), (2, 4), (3, 5)]
    completed = {1}
    r = next_runnables(blocks, edges, completed, running=set())
    assert r == {2, 3}
