from codepack import Delivery
from codepack.storages import FileStorage
import pytest
import json
import os


def test_file_storage(testdir_file_storage, dummy_deliveries):
    storage = FileStorage(item_type=Delivery, key='_name', path=testdir_file_storage)
    assert storage.key == '_name'
    storage.save(item=dummy_deliveries)
    assert sorted(os.listdir(testdir_file_storage)) == sorted(['%s.json' % d.get_name() for d in dummy_deliveries])
    with pytest.raises(ValueError):
        storage.save(item=dummy_deliveries[0])
    assert dummy_deliveries[0].item == 'x'
    dummy_deliveries[0].send(item='z')
    storage.save(item=dummy_deliveries[0], update=True)
    storage.remove(key=[dummy_deliveries[1].get_name(), dummy_deliveries[2].get_name()])
    assert storage.exist(key=[d.get_name() for d in dummy_deliveries]) == [True, False, False]
    assert storage.exist(key=[d.get_name() for d in dummy_deliveries], summary='and') is False
    assert storage.exist(key=[d.get_name() for d in dummy_deliveries], summary='or') is True
    storage.save(item=[dummy_deliveries[1], dummy_deliveries[2]])
    assert storage.exist(key=[d.get_name() for d in dummy_deliveries]) == [True, True, True]
    assert storage.exist(key=[d.get_name() for d in dummy_deliveries], summary='and') is True
    assert storage.exist(key=[d.get_name() for d in dummy_deliveries], summary='or') is True
    search_result = storage.search(key='item', value=json.dumps('?'))
    assert type(search_result) == list and len(search_result) == 0
    ref = sorted([dummy_deliveries[1].get_name(), dummy_deliveries[2].get_name()])
    search_result = storage.search(key='item', value=json.dumps('z'))
    assert len(search_result) == 1 and search_result[0].get_name() == dummy_deliveries[0].get_name()
    search_result = storage.search(key='item', value=json.dumps('y'))
    assert len(search_result) == 2
    assert sorted([s.get_name() for s in search_result]) == ref
    search_result = storage.search(key='item', value=json.dumps('y'), to_dict=True)
    assert len(search_result) == 2
    assert sorted([s['_name'] for s in search_result]) == ref
    assert type(search_result[0]) == dict and type(search_result[1]) == dict
    assert set(search_result[0].keys()) == {'item', '_name', '_id', '_timestamp', '_serial_number'}
    assert set(search_result[1].keys()) == {'item', '_name', '_id', '_timestamp', '_serial_number'}
    search_result = storage.search(key='item', value=json.dumps('y'), projection=['item'])
    assert len(search_result) == 2
    assert type(search_result[0]) == dict and type(search_result[1]) == dict
    assert set(search_result[0].keys()) == {'item'}
    assert set(search_result[1].keys()) == {'item'}
    load_result = storage.load(key='???')
    assert load_result is None
    load_result = storage.load(key=['!??', '?!?', '??!'])
    assert type(load_result) == list and len(load_result) == 0
    load_result = storage.load(key=[d.get_name() for d in dummy_deliveries])
    assert type(load_result) == list and len(load_result) == len(dummy_deliveries)
    for i in range(len(load_result)):
        assert load_result[i].get_name() == dummy_deliveries[i].get_name()
        assert load_result[i].item == dummy_deliveries[i].item
        assert load_result[i].get_serial_number() == dummy_deliveries[i].get_serial_number()
    load_result = storage.load(key=dummy_deliveries[1].get_name())
    assert isinstance(load_result, Delivery)
    load_result = storage.load(key=[dummy_deliveries[1].get_name(), dummy_deliveries[2].get_name(), '???'], to_dict=True)
    assert type(load_result) == list and len(load_result) == 2
    assert type(load_result[0]) == dict and type(load_result[1]) == dict
    assert set(load_result[0].keys()) == {'item', '_name', '_id', '_timestamp', '_serial_number'}
    assert set(load_result[1].keys()) == {'item', '_name', '_id', '_timestamp', '_serial_number'}
    load_result = storage.load(key=[dummy_deliveries[1].get_name(), dummy_deliveries[2].get_name()],
                               projection=['_timestamp', '_id'])
    assert type(load_result) == list
    assert type(load_result[0]) == dict and type(load_result[1]) == dict
    assert set(load_result[0].keys()) == {'_id', '_timestamp'}
    assert set(load_result[1].keys()) == {'_id', '_timestamp'}
    storage.new_path = True
    storage.close()
    assert not os.path.exists(testdir_file_storage)


def test_file_storage_list_all(testdir_file_storage, dummy_deliveries):
    storage = FileStorage(item_type=Delivery, key='_name', path=testdir_file_storage)
    assert storage.key == '_name'
    storage.save(item=dummy_deliveries)
    tmp = sorted(os.listdir(testdir_file_storage))
    assert tmp == sorted(['%s.json' % d.get_name() for d in dummy_deliveries])
    dummy_keys = [d.get_name() for d in dummy_deliveries]
    all_keys = storage.list_all()
    assert sorted(all_keys) == sorted(dummy_keys)
    all_items = storage.load(key=all_keys)
    assert type(all_items) == list and len(all_items) == 3
    assert sorted([d.get_name() for d in all_items]) == sorted(all_keys)
    storage.remove(key=all_keys)
    assert len(os.listdir(testdir_file_storage)) == len(storage.list_all()) == 0


def test_file_storage_text_key_search(testdir_file_storage, dummy_deliveries_for_text_key_search):
    storage = FileStorage(item_type=Delivery, key='_name', path=testdir_file_storage)
    assert storage.key == '_name'
    assert storage.key == '_name'
    storage.save(item=dummy_deliveries_for_text_key_search)
    dummy_keys = sorted([d.get_name() for d in dummy_deliveries_for_text_key_search])
    all_keys = sorted(storage.list_all())
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
    assert len(os.listdir(testdir_file_storage)) == len(storage.text_key_search('banana')) == 0
