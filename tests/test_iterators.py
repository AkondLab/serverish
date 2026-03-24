from serverish.base.iterators import AsyncRangeIter, AsyncListIter, AsyncDictItemsIter, AsyncEnumerateIter


async def test_async_range_iter():
    target_list = [1, 2, 3, 4, 5]
    new_list = []
    async for n in AsyncRangeIter(1, 5):
        new_list.append(n)
    assert target_list == new_list


async def test_async_list_iter():
    target_list = [1, 2, 3, 4, 5]
    new_list = []
    async for n in AsyncListIter(target_list):
        new_list.append(n)
    assert target_list == new_list


async def test_async_dict_items_iter():
    target_dict = {'a': 2, 'b': 55}
    new_dict = {}
    async for n, m in AsyncDictItemsIter(target_dict):
        new_dict[n] = m
    assert target_dict == new_dict


async def test_async_enumerate_items_iter():
    target_dict = {0: 1, 1: 2, 2: 3}
    new_dict = {}
    async for n, m in AsyncEnumerateIter([m for n, m in target_dict.items()]):
        new_dict[n] = m
    assert target_dict == new_dict
