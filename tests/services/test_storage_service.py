from codepack import Code, CodePack, ArgPack, StorageService
from codepack.storages import MemoryStorage, FileStorage, MongoStorage
from tests import add2, mul2, forward, hello
import os


def test_memory_storage_service_check(default_os_env):
    storage = MemoryStorage(item_type=Code)
    assert storage.key is None
    mss = StorageService(storage=storage)
    assert storage.key is None
    mss.storage.init()
    code1 = Code(hello, name='hello1', storage_service=mss)
    code2 = Code(hello, name='hello2', storage_service=mss)
    code3 = Code(hello, name='hello3', storage_service=mss)
    code1.save()
    assert isinstance(mss.check(name=code1.get_name()), bool)
    assert code1.get_name() in storage.memory
    mss.remove(name=code1.get_name())
    code2.save()
    code3.save()
    assert len(storage.memory) == 2
    check = mss.check([code1.get_name(), code2.get_name(), code3.get_name()])
    assert isinstance(check, list)
    assert len(check) == 3
    assert not check[0] and check[1] and check[2]


def test_file_storage_service_check(default_os_env, testdir_storage_service):
    storage = FileStorage(item_type=Code, path=testdir_storage_service)
    assert storage.key is None
    fss = StorageService(storage=storage)
    assert storage.key is None
    code1 = Code(hello, name='hello1', storage_service=fss)
    code2 = Code(hello, name='hello2', storage_service=fss)
    code3 = Code(hello, name='hello3', storage_service=fss)
    for i in ['hello1', 'hello2', 'hello3']:
        try:
            fss.remove(name=i)
        except Exception:
            continue
    code1.save()
    code2.save()
    check = fss.check([code1.get_name(), code2.get_name(), code3.get_name()])
    assert isinstance(check, list)
    assert check[0] and check[1] and not check[2]
    fss.remove(name=code1.get_name())
    code3.save()
    check = fss.check([code1.get_name(), code2.get_name(), code3.get_name()])
    assert isinstance(check, list)
    assert not check[0] and check[1] and check[2]
    assert len(check) == 3


def test_mongo_storage_service_check(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'codes'
    storage = MongoStorage(item_type=Code, mongodb=fake_mongodb, db=db, collection=collection)
    assert storage.key is None
    mss = StorageService(storage=storage)
    assert storage.key is None
    code1 = Code(hello, name='hello1', storage_service=mss)
    code2 = Code(hello, name='hello2', storage_service=mss)
    code3 = Code(hello, name='hello3', storage_service=mss)
    for i in ['hello1', 'hello2', 'hello3']:
        try:
            mss.remove(name=i)
        except Exception:
            continue
    code1.save()
    code2.save()
    check = mss.check([code1.get_name(), code2.get_name(), code3.get_name()])
    assert isinstance(check, list)
    assert check[0] and check[1] and not check[2]
    mss.remove(name=code1.get_name())
    code3.save()
    check = mss.check([code1.get_name(), code2.get_name(), code3.get_name()])
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
    assert code1.get_name() in storage.memory
    assert code2.get_name() not in storage.memory
    code3 = mss.load(code1.get_name())
    assert code1.get_name() == code3.get_name()
    assert code1.source.strip() == code3.source.strip()
    assert code1("CodePack") == code3("CodePack")
    mss.remove(code3.get_name())
    assert len(storage.memory) == 0


def test_file_storage_service(default_os_env, testdir_storage_service):
    filepath = testdir_storage_service
    storage = FileStorage(item_type=Code, path=filepath)
    fss = StorageService(storage=storage)
    assert storage.path == filepath
    code1 = Code(hello, storage_service=fss)
    code2 = Code(add2, storage_service=fss)
    code1.save()
    assert not os.path.isfile(Code.get_path(key=code1.get_name()))
    assert os.path.isfile(Code.get_path(key=code1.get_name(), path=filepath))
    assert not os.path.isfile(Code.get_path(key=code2.get_name(), path=filepath))
    code3 = fss.load(code1.get_name())
    assert code1.get_name() == code3.get_name()
    assert code1.source.strip() == code3.source.strip()
    assert code1("CodePack") == code3("CodePack")
    fss.remove(code3.get_name())
    assert not os.path.isfile(Code.get_path(key=code1.get_name(), path=filepath))


def test_mongo_storage_service(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'codes'
    storage = MongoStorage(item_type=Code, mongodb=fake_mongodb, db=db, collection=collection)
    mss = StorageService(storage=storage)
    assert mss.storage.item_type == Code
    code1 = Code(hello, storage_service=mss)
    code2 = Code(add2, storage_service=mss)
    code1.save()
    assert storage.mongodb[db][collection].count_documents({'_id': code1.get_name()}) == 1
    assert storage.mongodb[db][collection].count_documents({'_id': code2.get_name()}) == 0
    assert storage.item_type == Code
    code3 = mss.load(code1.get_name())
    assert code1.get_name() == code3.get_name()
    assert code1.source.strip() == code3.source.strip()
    assert code1("CodePack") == code3("CodePack")
    mss.remove(code3.get_name())
    assert storage.mongodb[db][collection].count_documents({'_id': code1.get_name()}) == 0


def test_storage_service_code_version(default_os_env, testdir_storage_service):
    filepath = testdir_storage_service
    storage = FileStorage(item_type=Code, path=filepath)
    fss = StorageService(storage=storage)
    assert storage.path == filepath
    assert storage.key is None
    code1 = Code(hello, storage_service=fss, version='0.1.1')
    code2 = Code(add2, storage_service=fss, name='haha@123')
    assert code1.get_name() == 'hello@0.1.1'
    assert code1.get_version() == '0.1.1'
    assert code2.get_name() == 'haha@123'
    assert code2.get_version() == '123'
    fss.save(code1)
    fss.save(code2)
    assert os.path.isfile(Code.get_path(key='hello@0.1.1', path=filepath))
    assert os.path.isfile(Code.get_path(key='haha@123', path=filepath))
    code3 = fss.load(name='hello@0.1.1')
    assert code3.get_name() == 'hello@0.1.1'
    assert code3.get_version() == '0.1.1'


def test_storage_service_codepack_version(default_os_env, testdir_storage_service):
    filepath = testdir_storage_service
    storage = FileStorage(item_type=CodePack, path=filepath)
    fss = StorageService(storage=storage)
    assert storage.path == filepath
    assert storage.key is None
    code1 = Code(add2, version='0.1.1')
    code2 = Code(mul2, name='haha@123')
    code3 = Code(hello, name='hoho', version='0.1.2')
    code4 = Code(forward)
    code1 >> code2
    code3 >> code4
    codepack1 = CodePack(name='hello', version='0.5.1', code=code1, subscribe=code2, storage_service=fss)
    codepack2 = CodePack(name='haha@1.2.3', code=code3, subscribe=code4, storage_service=fss)
    assert codepack1.get_name() == 'hello@0.5.1'
    assert codepack1.get_version() == '0.5.1'
    assert codepack2.get_name() == 'haha@1.2.3'
    assert codepack2.get_version() == '1.2.3'
    fss.save(codepack1)
    fss.save(codepack2)
    assert os.path.isfile(CodePack.get_path(key='hello@0.5.1', path=filepath))
    assert os.path.isfile(CodePack.get_path(key='haha@1.2.3', path=filepath))
    codepack3 = fss.load(name='hello@0.5.1')
    assert codepack3.get_version() == '0.5.1'
    assert codepack3.get_name() == 'hello@0.5.1'
    assert set(codepack3.codes.keys()) == {'add2@0.1.1', 'haha@123'}
    codepack4 = fss.load(name='haha@1.2.3')
    assert codepack4.get_version() == '1.2.3'
    assert codepack4.get_name() == 'haha@1.2.3'
    assert set(codepack4.codes.keys()) == {'hoho@0.1.2', 'forward'}


def test_storage_service_argpack_version(default_os_env, testdir_storage_service):
    filepath = testdir_storage_service
    storage = FileStorage(item_type=ArgPack, path=filepath)
    fss = StorageService(storage=storage)
    assert storage.path == filepath
    assert storage.key is None
    code1 = Code(add2, version='0.1.1')
    code2 = Code(mul2, name='haha@123')
    code3 = Code(hello, name='hoho', version='0.1.2')
    code4 = Code(forward)
    code1 >> code2
    code3 >> code4
    codepack1 = CodePack(name='hello', version='0.5.1', code=code1, subscribe=code2, storage_service=fss)
    codepack2 = CodePack(name='haha@1.2.3', code=code3, subscribe=code4, storage_service=fss)
    argpack1 = codepack1.make_argpack()
    argpack2 = codepack2.make_argpack()
    argpack1.set_version('1.2.3')
    argpack2.set_version('4.5.6')
    assert argpack1.get_name() == 'hello@1.2.3'
    assert argpack1.get_version() == '1.2.3'
    assert argpack2.get_name() == 'haha@4.5.6'
    assert argpack2.get_version() == '4.5.6'
    fss.save(argpack1)
    fss.save(argpack2)
    assert os.path.isfile(CodePack.get_path(key='hello@1.2.3', path=filepath))
    assert os.path.isfile(CodePack.get_path(key='haha@4.5.6', path=filepath))
    argpack3 = fss.load(name='hello@1.2.3')
    assert argpack3.get_version() == '1.2.3'
    assert argpack3.get_name() == 'hello@1.2.3'
    argpack4 = fss.load(name='haha@4.5.6')
    assert argpack4.get_version() == '4.5.6'
    assert argpack4.get_name() == 'haha@4.5.6'
