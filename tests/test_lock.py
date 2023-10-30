from codepack.jobs.lock import Lock
from codepack.jobs.async_lock import AsyncLock
from tests import run_function
import pytest


test_params = ("lock_class", [Lock, AsyncLock])


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_if_lock_is_retrieved(lock_class, init_storages):
    lock1 = lock_class(key='test1')
    print(lock1.get_storage())
    assert await run_function(lock1.retrieve)
    lock2 = lock_class(key='test1')
    assert not await run_function(lock2.retrieve)
    lock3 = lock_class(key='test2')
    assert await run_function(lock3.retrieve)


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_lock_serialization(lock_class):
    lock1 = lock_class(key='test')
    lock1_dict = lock1.to_dict()
    lock2 = lock_class.from_dict(lock1_dict)
    assert lock1_dict == lock2.to_dict()
