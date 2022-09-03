from codepack import Delivery
from codepack.storages import MemoryStorage
import pytest
import json


def test_memory_storage(dummy_deliveries):
    storage = MemoryStorage(item_type=Delivery, key='id')
    assert storage.key == 'id'
    storage.save(item=dummy_deliveries)
    assert sorted(storage.memory.keys()) == sorted([d.get_id() for d in dummy_deliveries])
    with pytest.raises(ValueError):
        storage.save(item=dummy_deliveries[0])
    assert dummy_deliveries[0].item == 'x'
    dummy_deliveries[0].send(item='z')
    storage.save(item=dummy_deliveries[0], update=True)
    storage.remove(key=[dummy_deliveries[1].get_id(), dummy_deliveries[2].get_id()])
    assert storage.exist(key=[d.get_id() for d in dummy_deliveries]) == [True, False, False]
    assert storage.exist(key=[d.get_id() for d in dummy_deliveries], summary='and') is False
    assert storage.exist(key=[d.get_id() for d in dummy_deliveries], summary='or') is True
    storage.save(item=[dummy_deliveries[1], dummy_deliveries[2]])
    assert storage.exist(key=[d.get_id() for d in dummy_deliveries]) == [True, True, True]
    assert storage.exist(key=[d.get_id() for d in dummy_deliveries], summary='and') is True
    assert storage.exist(key=[d.get_id() for d in dummy_deliveries], summary='or') is True
    search_result = storage.search(key='item', value=json.dumps('?'))
    assert type(search_result) == list and len(search_result) == 0
    ref = sorted([dummy_deliveries[1].get_id(), dummy_deliveries[2].get_id()])
    search_result = storage.search(key='item', value=json.dumps('z'))
    assert len(search_result) == 1 and search_result[0] == dummy_deliveries[0]
    search_result = storage.search(key='item', value=json.dumps('y'))
    assert len(search_result) == 2
    assert sorted([s.get_id() for s in search_result]) == ref
    search_result = storage.search(key='item', value=json.dumps('y'), to_dict=True)
    assert len(search_result) == 2
    assert sorted([s['id'] for s in search_result]) == ref
    assert type(search_result[0]) == dict and type(search_result[1]) == dict
    assert set(search_result[0].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    assert set(search_result[1].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    search_result = storage.search(key='item', value=json.dumps('y'), projection=['item'])
    assert len(search_result) == 2
    assert type(search_result[0]) == dict and type(search_result[1]) == dict
    assert set(search_result[0].keys()) == {'item'}
    assert set(search_result[1].keys()) == {'item'}
    load_result = storage.load(key='???')
    assert load_result is None
    load_result = storage.load(key=['!??', '?!?', '??!'])
    assert type(load_result) == list and len(load_result) == 0
    load_result = storage.load(key=[d.get_id() for d in dummy_deliveries])
    assert type(load_result) == list and load_result == dummy_deliveries
    load_result = storage.load(key=dummy_deliveries[1].get_id())
    assert isinstance(load_result, Delivery)
    load_result = storage.load(key=[dummy_deliveries[1].get_id(), dummy_deliveries[2].get_id(), '???'], to_dict=True)
    assert type(load_result) == list and len(load_result) == 2
    assert type(load_result[0]) == dict and type(load_result[1]) == dict
    assert set(load_result[0].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    assert set(load_result[1].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    load_result = storage.load(key=[dummy_deliveries[1].get_id(), dummy_deliveries[2].get_id()], projection=['timestamp', '_id'])
    assert type(load_result) == list
    assert type(load_result[0]) == dict and type(load_result[1]) == dict
    assert set(load_result[0].keys()) == {'_id', 'timestamp'}
    assert set(load_result[1].keys()) == {'_id', 'timestamp'}
    storage.close()
    assert not storage.memory


def test_memory_storage_list_all(dummy_deliveries):
    storage = MemoryStorage(item_type=Delivery, key='id')
    assert storage.key == 'id'
    storage.save(item=dummy_deliveries)
    dummy_keys = sorted([d.get_id() for d in dummy_deliveries])
    all_keys = sorted(storage.list_all())
    assert sorted(storage.memory.keys()) == dummy_keys
    assert all_keys == dummy_keys
    all_items = storage.load(key=all_keys)
    assert type(all_items) == list and len(all_items) == 3
    assert sorted([d.get_id() for d in all_items]) == all_keys
    storage.remove(key=all_keys)
    assert len(storage.memory) == len(storage.list_all()) == 0


def test_memory_storage_text_key_search(dummy_deliveries_for_text_key_search):
    storage = MemoryStorage(item_type=Delivery, key='id')
    assert storage.key == 'id'
    storage.save(item=dummy_deliveries_for_text_key_search)
    dummy_keys = sorted([d.get_id() for d in dummy_deliveries_for_text_key_search])
    all_keys = sorted(storage.list_all())
    assert sorted(storage.memory.keys()) == dummy_keys
    assert all_keys == dummy_keys
    banana_items = storage.text_key_search(key='banana')
    assert isinstance(banana_items, list)
    assert len(banana_items) == 2
    assert sorted(banana_items) == ['banana_apple', 'orange_banana']
    apple_items = storage.text_key_search(key='apple')
    assert sorted(apple_items) == ['apple_orange', 'banana_apple']
    _items = storage.text_key_search(key='_')
    assert sorted(_items) == ['apple_orange', 'banana_apple', 'orange_banana']
    storage.remove(key=all_keys)
    assert len(storage.memory) == len(storage.text_key_search('banana')) == 0
