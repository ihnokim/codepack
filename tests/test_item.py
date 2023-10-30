from typing import Any, Dict
from tests import add2, mul2
from codepack.item import Item
from codepack.code import Code
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.storages.memory_storage import MemoryStorage


def test_item_name_with_versioning():
    code = Code(add2, version='0.0.1')
    assert code.name == 'add2'
    assert code.version == '0.0.1'
    assert code.get_id() == 'add2@0.0.1'
    code.name = 'test'
    code.version = '0.0.2'
    assert code.get_id() == 'test@0.0.2'


def test_if_storage_is_managed_independently_for_each_class():
    Code.storage = None
    CodeSnapshot.storage = None
    ms1 = MemoryStorage()
    ms2 = MemoryStorage()
    assert Code.storage is None
    assert CodeSnapshot.storage is None
    code1 = Code(add2)
    code2 = Code(mul2)
    snapshot = CodeSnapshot(name='test_name', function=code1.function)
    assert code1.storage is None
    assert code2.storage is None
    assert snapshot.storage is None
    Code.set_storage(storage=ms1)
    assert Code.storage == ms1
    assert code1.storage == ms1
    assert code2.storage == ms1
    assert CodeSnapshot.storage is None
    assert snapshot.storage is None
    snapshot.set_storage(ms2)
    assert Code.storage == ms1
    assert code1.storage == ms1
    assert code2.storage == ms1
    assert CodeSnapshot.storage == ms2
    assert snapshot.storage == ms2


def test_default_get_id():
    class TestItem(Item):
        def __serialize__(self) -> Dict[str, Any]:
            return {'test': 123}

        @classmethod
        def __deserialize__(cls, d: Dict[str, Any]) -> Item:
            return cls()

    test_item = TestItem()
    assert test_item.get_id() == test_item._id
    assert test_item.to_dict() == {'test': 123, '_id': test_item._id}
