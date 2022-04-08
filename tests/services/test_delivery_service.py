from codepack import DeliveryService, Delivery
from codepack.storages import MemoryStorage, FileStorage, MongoStorage
import os


def test_memory_delivery_service_check():
    storage = MemoryStorage(item_type=Delivery)
    mds = DeliveryService(storage=storage)
    mds.storage.init()
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert isinstance(mds.check(serial_number=serial_number), bool)
    mds.cancel(serial_number=serial_number)
    serial_numbers = ['123', '456', '789']
    mds.send(id=sender, serial_number=serial_numbers[0], item=item)
    mds.send(id=sender, serial_number=serial_numbers[1], item=item)
    check = mds.check(serial_number=serial_numbers)
    assert isinstance(check, bool)
    assert check is False
    mds.send(id=sender, serial_number=serial_numbers[2], item=item)
    assert mds.check(serial_number=serial_numbers) is True


def test_file_delivery_service_check(testdir_delivery_service):
    storage = FileStorage(item_type=Delivery, path=testdir_delivery_service)
    fds = DeliveryService(storage=storage)
    sender = 'sender'
    serial_number = '1234'
    serial_numbers = ['123', '456', '789']
    try:
        fds.cancel(serial_number=serial_number)
    except Exception:
        pass
    for i in serial_numbers:
        try:
            fds.cancel(serial_number=i)
        except Exception:
            continue
    item = 'Hello, World!'
    fds.send(id=sender, serial_number=serial_number, item=item)
    check = fds.check(serial_number=serial_number)
    assert isinstance(check, bool)
    assert check is True
    fds.cancel(serial_number=serial_number)
    check = fds.check(serial_number=serial_number)
    assert check is False
    fds.send(id=sender, serial_number=serial_numbers[0], item=item)
    fds.send(id=sender, serial_number=serial_numbers[1], item=item)
    check = fds.check(serial_number=serial_numbers)
    assert isinstance(check, bool)
    assert check is False
    fds.send(id=sender, serial_number=serial_numbers[2], item=item)
    check = fds.check(serial_number=serial_numbers)
    assert check is True


def test_mongo_delivery_service_check(fake_mongodb):
    db = 'test'
    collection = 'cache'
    storage = MongoStorage(item_type=Delivery, mongodb=fake_mongodb, db=db, collection=collection)
    mds = DeliveryService(storage=storage)
    sender = 'sender'
    serial_number = '1234'
    serial_numbers = ['123', '456', '789']
    mds.cancel(serial_number=serial_number)
    for i in serial_numbers:
        mds.cancel(serial_number=i)
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    check = mds.check(serial_number=serial_number)
    assert isinstance(check, bool)
    assert check is True
    mds.cancel(serial_number=serial_number)
    mds.send(id=sender, serial_number=serial_numbers[0], item=item)
    mds.send(id=sender, serial_number=serial_numbers[1], item=item)
    check = mds.check(serial_number=serial_numbers)
    assert isinstance(check, bool)
    assert check is False
    mds.send(id=sender, serial_number=serial_numbers[2], item=item)
    assert mds.check(serial_number=serial_numbers) is True


def test_memory_delivery_service():
    storage = MemoryStorage(item_type=Delivery)
    mds = DeliveryService(storage=storage)
    mds.storage.init()
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert serial_number in storage.memory, "'send' failed"
    check = mds.check(serial_number=serial_number)
    receive = mds.receive(serial_number=serial_number)
    assert check is True
    assert receive == item, "'receive' failed"
    mds.cancel(serial_number=serial_number)
    check2 = mds.check(serial_number=serial_number)
    assert not check2, "'cancel' failed"


def test_file_delivery_service(testdir_delivery_service):
    storage = FileStorage(item_type=Delivery, path=testdir_delivery_service)
    fds = DeliveryService(storage=storage)
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    fds.send(id=sender, serial_number=serial_number, item=item)
    assert os.path.isfile(Delivery.get_path(key=serial_number, path=storage.path)), "'send' failed"
    check = fds.check(serial_number=serial_number)
    receive = fds.receive(serial_number=serial_number)
    assert check is True
    assert receive == item, "'receive' error"
    fds.cancel(serial_number=serial_number)
    check2 = fds.check(serial_number=serial_number)
    assert check2 is False, "'cancel' error"


def test_mongo_delivery_service(fake_mongodb):
    db = 'test'
    collection = 'cache'
    storage = MongoStorage(item_type=Delivery, mongodb=fake_mongodb, db=db, collection=collection)
    mds = DeliveryService(storage=storage)
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert fake_mongodb[db][collection].find_one({'_id': serial_number}), "'send' failed"
    check = mds.check(serial_number=serial_number)
    receive = mds.receive(serial_number=serial_number)
    assert check is True, "'check' failed"
    assert receive == item, "'receive' error"
    mds.cancel(serial_number=serial_number)
    check2 = mds.check(serial_number=serial_number)
    assert check2 is False, "'cancel' error"
