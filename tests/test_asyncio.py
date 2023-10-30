import pytest
import os
from codepack.async_code import AsyncCode
from codepack.async_function import AsyncFunction
from codepack.storages.async_memory_storage import AsyncMemoryStorage
from codepack.storages.storage_type import StorageType
from tests import add2, async_add2


@pytest.mark.asyncio
async def test_async_code_local_execution():
    code1 = AsyncCode(add2)
    code2 = AsyncCode(async_add2)
    assert isinstance(code1.function, AsyncFunction)
    assert isinstance(code2.function, AsyncFunction)
    assert await code1(1, 3) == 4
    assert await code2(2, 3) == 5


@pytest.mark.asyncio
async def test_save_async_code_without_anything():
    code = AsyncCode(async_add2)
    with pytest.raises(KeyError):
        await code.save()


@pytest.mark.asyncio
async def test_save_and_load_async_code_with_async_memory_storage():
    os_envs = {'CODEPACK__ASYNCCODE__TYPE': 'async_memory_storage',
               'CODEPACK__ASYNCCODE__CONFIG': 'ams',
               'CODEPACK__AMS__KEEP_ORDER': 'true'}
    try:
        for os_env, value in os_envs.items():
            os.environ[os_env] = value
        StorageType.register('async_memory_storage', AsyncMemoryStorage)
        code1 = AsyncCode(async_add2)
        assert isinstance(code1.function, AsyncFunction)
        assert await code1.save() is True
        assert isinstance(code1.get_storage(), AsyncMemoryStorage)
        code2 = await AsyncCode.load('async_add2')
        assert isinstance(code2.function, AsyncFunction)
        assert code1.function.source.strip() == code2.function.source.strip()
    finally:
        for os_env in os_envs:
            os.environ.pop(os_env)
        StorageType.unregister('async_memory_storage')
