# This is generic class implementing a set of specified capacity.
# When the capacity is reach during the insertion of a new element,
# the oldest element is removed from the set.

from typing import TypeVar, Generic
from collections import deque


# The type T of the set elements:
T = TypeVar('T')


class FifoSet(Generic[T]):
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = set()
        self.order = deque()

    def __contains__(self, item: T) -> bool:
        return item in self.cache

    def __len__(self) -> int:
        return len(self.cache)

    def __iter__(self):
        return iter(self.cache)

    def add(self, item: T):
        if item in self.cache:
            return
        if len(self.cache) >= self.capacity:
            self.cache.remove(self.order.popleft())
        self.cache.add(item)
        self.order.append(item)

    def remove(self, item: T):
        self.cache.remove(item)
        self.order.remove(item)

    def clear(self):
        self.cache.clear()
        self.order.clear()
