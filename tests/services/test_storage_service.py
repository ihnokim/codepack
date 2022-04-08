from codepack import Code, StorageService
from codepack.storages import MemoryStorage, FileStorage, MongoStorage
from tests import *
import os


def test_memory_storage_service_check(default_os_env):
    storage = MemoryStorage(item_type=Code)
    assert storage.key == 'serial_number'
    mss = StorageService(storage=storage)
    assert storage.key == 'id'
    mss.storage.init()
    code1 = Code(hello, id='hello1', storage_service=mss)
    code2 = Code(hello, id='hello2', storage_service=mss)
    code3 = Code(hello, id='hello3', storage_service=mss)
    code1.save()
    assert isinstance(mss.check(id=code1.id), bool)
    assert code1.id in storage.memory
    mss.remove(id=code1.id)
    code2.save()
    code3.save()
    assert len(storage.memory) == 2
    check = mss.check([code1.id, code2.id, code3.id])
    assert isinstance(check, list)
    assert len(check) == 3
    assert not check[0] and check[1] and check[2]


def test_file_storage_service_check(default_os_env, testdir_storage_service):
    storage = FileStorage(item_type=Code, path=testdir_storage_service)
    assert storage.key == 'serial_number'
    fss = StorageService(storage=storage)
    assert storage.key == 'id'
    code1 = Code(hello, id='hello1', storage_service=fss)
    code2 = Code(hello, id='hello2', storage_service=fss)
    code3 = Code(hello, id='hello3', storage_service=fss)
    for i in ['hello1', 'hello2', 'hello3']:
        try:
            fss.remove(id=i)
        except Exception:
            continue
    code1.save()
    code2.save()
    check = fss.check([code1.id, code2.id, code3.id])
    assert isinstance(check, list)
    assert check[0] and check[1] and not check[2]
    fss.remove(id=code1.id)
    code3.save()
    check = fss.check([code1.id, code2.id, code3.id])
    assert isinstance(check, list)
    assert not check[0] and check[1] and check[2]
    assert len(check) == 3


def test_mongo_storage_service_check(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'codes'
    storage = MongoStorage(item_type=Code, mongodb=fake_mongodb, db=db, collection=collection)
    assert storage.key == 'serial_number'
    mss = StorageService(storage=storage)
    assert storage.key == 'id'
    code1 = Code(hello, id='hello1', storage_service=mss)
    code2 = Code(hello, id='hello2', storage_service=mss)
    code3 = Code(hello, id='hello3', storage_service=mss)
    for i in ['hello1', 'hello2', 'hello3']:
        try:
            mss.remove(id=i)
        except Exception:
            continue
    code1.save()
    code2.save()
    check = mss.check([code1.id, code2.id, code3.id])
    assert isinstance(check, list)
    assert check[0] and check[1] and not check[2]
    mss.remove(id=code1.id)
    code3.save()
    check = mss.check([code1.id, code2.id, code3.id])
    assert isinstance(check, list)
    assert not check[0] and check[1] and check[2]
    assert len(check) == 3


def test_memory_storage_service(default_os_env):
    storage = MemoryStorage(item_type=Code)
    mss = StorageService(storage=storage)
    mss.storage.init()
    code1 = Code(hello, storage_service=mss)
    code2 = Code(add2, storage_service=mss)
    code1.save()
    assert len(storage.memory) == 1
    assert code1.id in storage.memory
    assert code2.id not in storage.memory
    code3 = mss.load(code1.id)
    assert code1.id == code3.id
    assert code1.source == code3.source
    assert code1("CodePack") == code3("CodePack")
    mss.remove(code3.id)
    assert len(storage.memory) == 0


def test_file_storage_service(default_os_env, testdir_storage_service):
    filepath = testdir_storage_service
    storage = FileStorage(item_type=Code, path=filepath)
    fss = StorageService(storage=storage)
    assert storage.path == filepath
    code1 = Code(hello, storage_service=fss)
    code2 = Code(add2, storage_service=fss)
    code1.save()
    assert not os.path.isfile(Code.get_path(key=code1.id))
    assert os.path.isfile(Code.get_path(key=code1.id, path=filepath))
    assert not os.path.isfile(Code.get_path(key=code2.id, path=filepath))
    code3 = fss.load(code1.id)
    assert code1.id == code3.id
    assert code1.source.strip() == code3.source.strip()
    assert code1("CodePack") == code3("CodePack")
    fss.remove(code3.id)
    assert not os.path.isfile(Code.get_path(key=code1.id, path=filepath))


def test_mongo_storage_service(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'codes'
    storage = MongoStorage(item_type=Code, mongodb=fake_mongodb, db=db, collection=collection)
    mss = StorageService(storage=storage)
    assert mss.storage.item_type == Code
    code1 = Code(hello, storage_service=mss)
    code2 = Code(add2, storage_service=mss)
    code1.save()
    assert storage.mongodb[db][collection].count_documents({'_id': code1.id}) == 1
    assert storage.mongodb[db][collection].count_documents({'_id': code2.id}) == 0
    assert storage.item_type == Code
    code3 = mss.load(code1.id)
    assert code1.id == code3.id
    assert code1.source.strip() == code3.source.strip()
    assert code1("CodePack") == code3("CodePack")
    mss.remove(code3.id)
    assert storage.mongodb[db][collection].count_documents({'_id': code1.id}) == 0
