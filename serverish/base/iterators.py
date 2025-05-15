import asyncio
from typing import List, Dict, Any, Tuple, Set


class AsyncRangeIter:
    """This class represent async range iterator (instead of sync).

    WARNING: Noticed that end is included (not like in standard iterator).
    Example 1: start=1, end=4, will return [1, 2, 3, 4]
    Example 2: async for n in AsyncRangeIter(start=1, end=3): ...
    """

    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end

    def __aiter__(self) -> Any:
        self.current = self.start
        return self

    async def __anext__(self) -> int:
        if self.current <= self.end:
            value = self.current
            self.current += 1
            await asyncio.sleep(0)
            return value
        else:
            raise StopAsyncIteration


class AsyncListIter:
    """This class represent async list iterator (instead of sync).

    Example of use: async for n in AsyncListIter(some_list): ...
    """
    def __init__(self, iterable: List | Tuple | Set | Any):
        self.iterable = iterable
        self.index: int = 0

    def __aiter__(self) -> Any:
        self.index = 0
        return self

    async def __anext__(self):
        if self.index < len(self.iterable):
            value = self.iterable[self.index]
            self.index += 1
            await asyncio.sleep(0)
            return value
        else:
            raise StopAsyncIteration


class AsyncEnumerateIter:
    """This class represent async enumerate iterator (instead of sync).

    Example of use: async for current_index, value in AsyncEnumerateIter(some_iterable): ...
    """

    def __init__(self, iterable: List | Tuple | Set | Any) -> None:
        self.iterable = iterable

    def __aiter__(self):
        self.index = 0
        return self

    async def __anext__(self):
        if self.index < len(self.iterable):
            value = self.iterable[self.index]
            current_index = self.index
            self.index += 1
            await asyncio.sleep(0)
            return current_index, value
        else:
            raise StopAsyncIteration


class AsyncDictItemsIter:
    """This class represent async dict items iterator (instead of sync).

    Example of use: async for key, value in AsyncEnumerateIter(some_dict): ...
    """

    def __init__(self, data_dict: Dict) -> None:
        self.data_dict = data_dict

    def __aiter__(self) -> Any:
        self.iterator = iter(self.data_dict.items())
        return self

    async def __anext__(self) -> Tuple:
        try:
            n, m = next(self.iterator)
            await asyncio.sleep(0)
            return n, m
        except StopIteration:
            raise StopAsyncIteration
