from codepack.storages.memory_storage import MemoryStorage
from codepack.storages.file_storage import FileStorage
from codepack.storages.async_file_storage import AsyncFileStorage
from codepack.storages.async_memory_storage import AsyncMemoryStorage
from codepack.interfaces.random_access_memory import RandomAccessMemory
from codepack.interfaces.file_interface import FileInterface
from codepack.interfaces.async_file_interface import AsyncFileInterface
from codepack.code import Code
from tests import add2, mul2, add3, run_function
import pytest


test_params = ("storage_class,config", [(MemoryStorage, {}),
                                        (FileStorage, {'path': 'testdir'}),
                                        (AsyncMemoryStorage, {'keep_order': 'True'}),
                                        (AsyncFileStorage, {'path': 'testdir'})
                                        ])


@pytest.mark.parametrize("config,storage_class,interface", [({'keep_order': 'True'},
                                                             MemoryStorage,
                                                             RandomAccessMemory),
                                                            ({'path': 'testdir'},
                                                             FileStorage,
                                                             FileInterface),
                                                            ({'keep_order': 'False'},
                                                             AsyncMemoryStorage,
                                                             RandomAccessMemory),
                                                            ({'path': 'testdir'},
                                                             AsyncFileStorage,
                                                             AsyncFileInterface),
                                                            ])
def test_initialization_with_config(config, storage_class, interface, testdir):
    storage = storage_class.get_instance(config=config)
    assert isinstance(storage.interface, interface)


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_load(storage_class, config, testdir):
    code = Code(add2)
    storage = storage_class(**config)
    assert await run_function(storage.load, id=code.get_id()) is None
    assert await run_function(storage.save, id=code.get_id(), item=code.to_dict())
    item = await run_function(storage.load, id=code.get_id())
    assert item == code.to_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_count(storage_class, config, testdir):
    code = Code(add2)
    storage = storage_class(**config)
    assert await run_function(storage.count) == 0
    await run_function(storage.save, id=code.get_id(), item=code.to_dict())
    assert await run_function(storage.count) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_count_like(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code4 = Code(add3, version='0.0.4')
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.save, id=code3.get_id(), item=code3.to_dict())
    await run_function(storage.save, id=code4.get_id(), item=code4.to_dict())
    assert await run_function(storage.count, id='add') == 3
    assert await run_function(storage.count, id='add3') == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_list_all(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    assert sorted(await run_function(storage.list_all)) == sorted([code1.get_id(), code2.get_id()])


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_list_like(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code4 = Code(add3, version='0.0.4')
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.save, id=code3.get_id(), item=code3.to_dict())
    await run_function(storage.save, id=code4.get_id(), item=code4.to_dict())
    assert sorted(await run_function(storage.list_like, id='add')) == sorted([code1.get_id(),
                                                                              code3.get_id(),
                                                                              code4.get_id()])
    assert sorted(await run_function(storage.list_like, id='add3')) == sorted([code3.get_id(),
                                                                               code4.get_id()])


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_redundant_save(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(add2)
    storage = storage_class(**config)
    is_saved1 = await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    is_saved2 = await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    assert is_saved1
    assert not is_saved2
    assert len(await run_function(storage.list_all)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_trying_updating_when_nothing_exists(storage_class, config, testdir):
    storage = storage_class(**config)
    is_updated = await run_function(storage.update, id='test', test_key='test_value')
    assert not is_updated
    assert len(await run_function(storage.list_all)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_update(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.update, id=code2.get_id(), description='Hello, World!')
    original_code1 = code1.to_dict()
    original_code2 = code2.to_dict()
    retrieved_code1 = await run_function(storage.load, id=code1.get_id())
    retrieved_code2 = await run_function(storage.load, id=code2.get_id())
    assert original_code1 == retrieved_code1
    assert original_code2 != retrieved_code2
    assert original_code2['description'] != retrieved_code2['description']
    assert retrieved_code2['description'] == 'Hello, World!'


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_remove_nothing(storage_class, config, testdir):
    code = Code(add2)
    storage = storage_class(**config)
    await run_function(storage.save, id=code.get_id(), item=code.to_dict())
    is_removed = await run_function(storage.remove, id='test')
    assert not is_removed
    assert len(await run_function(storage.list_all)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_save_and_remove(storage_class, config, testdir):
    code = Code(add2)
    storage = storage_class(**config)
    await run_function(storage.save, id=code.get_id(), item=code.to_dict())
    is_removed = await run_function(storage.remove, id=code.get_id())
    assert is_removed
    assert len(await run_function(storage.list_all)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_search(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2, version='0.0.1')
    code3 = Code(add2, version='0.0.2', description='test')
    code4 = Code(add3, version='0.0.1', description='test')
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.save, id=code3.get_id(), item=code3.to_dict())
    await run_function(storage.save, id=code4.get_id(), item=code4.to_dict())
    assert set(await run_function(storage.search, key='name', value='add2')) == {'add2', 'add2@0.0.2'}
    assert set(await run_function(storage.search, key='version', value='0.0.1')) == {'mul2@0.0.1', 'add3@0.0.1'}
    assert set(await run_function(storage.search, key='description', value='test')) == {'add2@0.0.2', 'add3@0.0.1'}


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_load_many(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.save, id=code3.get_id(), item=code3.to_dict())
    results = await run_function(storage.load_many, id=['add2', 'add3', 'test'])
    assert len(results) == 2
    assert code1.to_dict() == results[0]
    assert code3.to_dict() == results[1]


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_load_all(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.save, id=code3.get_id(), item=code3.to_dict())
    results = await run_function(storage.load_many)
    assert len(results) == 3
    assert {x['_id'] for x in results} == {code1.get_id(), code2.get_id(), code3.get_id()}


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_exists_many(storage_class, config, testdir):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    storage = storage_class(**config)
    await run_function(storage.save, id=code1.get_id(), item=code1.to_dict())
    await run_function(storage.save, id=code2.get_id(), item=code2.to_dict())
    await run_function(storage.save, id=code3.get_id(), item=code3.to_dict())
    results = await run_function(storage.exists_many, id=['add2', 'add3', 'test'])
    assert len(results) == 3
    assert results == [True, True, False]
