from codepack import Code
from codepack.service import MemoryStorageService, FileStorageService, MongoStorageService
from tests import *


def test_singleton_memory_storage_service(default_os_env):
    mss1 = MemoryStorageService()
    mss1.init()
    code = Code(hello)
    mss1.save(code)
    mss2 = MemoryStorageService()
    assert mss1 == mss2
    assert len(mss1.storage) == len(mss2.storage) == 1
    assert mss2.check(id=code.id)


def test_memory_storage_service_check(default_os_env):
    mss = MemoryStorageService()
    mss.init()
    code1 = Code(hello, id='hello1', storage_service=mss)
    code2 = Code(hello, id='hello2', storage_service=mss)
    code3 = Code(hello, id='hello3', storage_service=mss)
    code1.save()
    assert isinstance(mss.check(id=code1.id), dict)
    assert code1.id in mss.storage
    mss.remove(id=code1.id)
    code2.save()
    code3.save()
    assert len(mss.storage) == 2
    check = mss.check([code2.id, code3.id])
    assert isinstance(check, list)
    assert len(check) == 2


def test_file_storage_service_check(default_os_env):
    fss = FileStorageService(obj=Code, path='tmp/storage_service/')
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
    check = [list(x.keys())[0] for x in fss.check([code1.id, code2.id, code3.id])]
    assert isinstance(check, list)
    assert code1.id in check and code2.id in check and code3.id not in check
    fss.remove(id=code1.id)
    code3.save()
    check = [list(x.keys())[0] for x in fss.check([code1.id, code2.id, code3.id])]
    assert isinstance(check, list)
    assert code1.id not in check and code2.id in check and code3.id in check
    assert len(check) == 2


def test_mongo_storage_service_check(default_os_env, mongodb):
    db = 'test'
    collection = 'codes'
    mss = MongoStorageService(obj=Code, mongodb=mongodb, db=db, collection=collection)
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
    check = [list(x.keys())[0] for x in mss.check([code1.id, code2.id, code3.id])]
    assert isinstance(check, list)
    assert code1.id in check and code2.id in check and code3.id not in check
    mss.remove(id=code1.id)
    code3.save()
    check = [list(x.keys())[0] for x in mss.check([code1.id, code2.id, code3.id])]
    assert isinstance(check, list)
    assert code1.id not in check and code2.id in check and code3.id in check
    assert len(check) == 2
