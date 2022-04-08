from codepack import Delivery
from codepack.storages import FileStorage
import pytest
import json
import os


def test_file_storage(testdir_file_storage, dummy_deliveries):
    storage = FileStorage(item_type=Delivery, key='id', path=testdir_file_storage)
    assert storage.key == 'id'
    storage.save(item=dummy_deliveries)
    assert sorted(os.listdir(testdir_file_storage)) == sorted(['%s.json' % d.id for d in dummy_deliveries])
    with pytest.raises(ValueError):
        storage.save(item=dummy_deliveries[0])
    assert dummy_deliveries[0].item == 'x'
    dummy_deliveries[0].send(item='z')
    storage.save(item=dummy_deliveries[0], update=True)
    storage.remove(key=[dummy_deliveries[1].id, dummy_deliveries[2].id])
    assert storage.exist(key=[d.id for d in dummy_deliveries]) == [True, False, False]
    assert storage.exist(key=[d.id for d in dummy_deliveries], summary='and') is False
    assert storage.exist(key=[d.id for d in dummy_deliveries], summary='or') is True
    storage.save(item=[dummy_deliveries[1], dummy_deliveries[2]])
    assert storage.exist(key=[d.id for d in dummy_deliveries]) == [True, True, True]
    assert storage.exist(key=[d.id for d in dummy_deliveries], summary='and') is True
    assert storage.exist(key=[d.id for d in dummy_deliveries], summary='or') is True
    search_result = storage.search(key='item', value=json.dumps('?'))
    assert type(search_result) == list and len(search_result) == 0
    ref = sorted([dummy_deliveries[1].id, dummy_deliveries[2].id])
    search_result = storage.search(key='item', value=json.dumps('z'))
    assert len(search_result) == 1 and search_result[0].id == dummy_deliveries[0].id
    search_result = storage.search(key='item', value=json.dumps('y'))
    assert len(search_result) == 2
    assert sorted([s.id for s in search_result]) == ref
    search_result = storage.search(key='item', value=json.dumps('y'), to_dict=True)
    assert len(search_result) == 2
    assert sorted([s['id'] for s in search_result]) == ref
    assert type(search_result[0]) == dict and type(search_result[1]) == dict
    assert set(search_result[0].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    assert set(search_result[1].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    search_result = storage.search(key='item', value=json.dumps('y'), projection=['item'])
    assert len(search_result) == 2
    assert type(search_result[0]) == dict and type(search_result[1]) == dict
    assert set(search_result[0].keys()) == {'item', 'id'}
    assert set(search_result[1].keys()) == {'item', 'id'}
    load_result = storage.load(key='???')
    assert load_result is None
    load_result = storage.load(key=['!??', '?!?', '??!'])
    assert type(load_result) == list and len(load_result) == 0
    load_result = storage.load(key=[d.id for d in dummy_deliveries])
    assert type(load_result) == list and len(load_result) == len(dummy_deliveries)
    for i in range(len(load_result)):
        assert load_result[i].id == dummy_deliveries[i].id
        assert load_result[i].item == dummy_deliveries[i].item
        assert load_result[i].serial_number == dummy_deliveries[i].serial_number
    load_result = storage.load(key=dummy_deliveries[1].id)
    assert isinstance(load_result, Delivery)
    load_result = storage.load(key=[dummy_deliveries[1].id, dummy_deliveries[2].id, '???'], to_dict=True)
    assert type(load_result) == list and len(load_result) == 2
    assert type(load_result[0]) == dict and type(load_result[1]) == dict
    assert set(load_result[0].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    assert set(load_result[1].keys()) == {'item', 'id', '_id', 'timestamp', 'serial_number'}
    load_result = storage.load(key=[dummy_deliveries[1].id, dummy_deliveries[2].id], projection=['timestamp', '_id'])
    assert type(load_result) == list
    assert type(load_result[0]) == dict and type(load_result[1]) == dict
    assert set(load_result[0].keys()) == {'id', '_id', 'timestamp'}
    assert set(load_result[1].keys()) == {'id', '_id', 'timestamp'}
    storage.new_path = True
    storage.close()
    assert not os.path.exists(testdir_file_storage)


def test_file_storage_list_all(testdir_file_storage, dummy_deliveries):
    storage = FileStorage(item_type=Delivery, key='id', path=testdir_file_storage)
    assert storage.key == 'id'
    storage.save(item=dummy_deliveries)
    tmp = sorted(os.listdir(testdir_file_storage))
    assert tmp == sorted(['%s.json' % d.id for d in dummy_deliveries])
    dummy_keys = [d.id for d in dummy_deliveries]
    all_keys = storage.list_all()
    assert sorted(all_keys) == sorted(dummy_keys)
    all_items = storage.load(key=all_keys)
    assert type(all_items) == list and len(all_items) == 3
    assert sorted([d.id for d in all_items]) == sorted(all_keys)
    storage.remove(key=all_keys)
    assert len(os.listdir(testdir_file_storage)) == len(storage.list_all()) == 0
