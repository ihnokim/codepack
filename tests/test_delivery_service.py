from codepack.service import MemoryDeliveryService, FileDeliveryService, MongoDeliveryService
from codepack.utils.delivery import Delivery
import os


def test_memory_delivery_service_check():
    mds = MemoryDeliveryService(obj=Delivery)
    mds.init()
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert isinstance(mds.check(serial_number=serial_number), dict)
    mds.cancel(serial_number=serial_number)
    serial_numbers = ['123', '456', '789']
    mds.send(id=sender, serial_number=serial_numbers[0], item=item)
    mds.send(id=sender, serial_number=serial_numbers[1], item=item)
    check = mds.check(serial_number=serial_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_file_delivery_service_check(testdir_delivery_service):
    fds = FileDeliveryService(obj=Delivery, path=testdir_delivery_service)
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
    assert isinstance(fds.check(serial_number=serial_number), dict)
    fds.cancel(serial_number=serial_number)
    fds.send(id=sender, serial_number=serial_numbers[0], item=item)
    fds.send(id=sender, serial_number=serial_numbers[1], item=item)
    check = fds.check(serial_number=serial_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_mongo_delivery_service_check(fake_mongodb):
    db = 'test'
    collection = 'cache'
    mds = MongoDeliveryService(obj=Delivery, mongodb=fake_mongodb, db=db, collection=collection)
    sender = 'sender'
    serial_number = '1234'
    serial_numbers = ['123', '456', '789']
    mds.cancel(serial_number=serial_number)
    for i in serial_numbers:
        mds.cancel(serial_number=i)
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert isinstance(mds.check(serial_number=serial_number), dict)
    mds.cancel(serial_number=serial_number)
    mds.send(id=sender, serial_number=serial_numbers[0], item=item)
    mds.send(id=sender, serial_number=serial_numbers[1], item=item)
    check = mds.check(serial_number=serial_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_memory_delivery_service():
    mds = MemoryDeliveryService(obj=Delivery)
    mds.init()
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert serial_number in mds.memory, "'send' failed"
    check = mds.check(serial_number=serial_number)
    receive = mds.receive(serial_number=serial_number)
    assert check['id'] == sender and check['_id'] == serial_number, "'check' failed"
    assert receive == item, "'receive' failed"
    mds.cancel(serial_number=serial_number)
    check2 = mds.check(serial_number=serial_number)
    assert not check2, "'cancel' failed"


def test_file_delivery_service(testdir_delivery_service):
    fds = FileDeliveryService(obj=Delivery, path=testdir_delivery_service)
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    fds.send(id=sender, serial_number=serial_number, item=item)
    assert os.path.isfile(Delivery.get_path(serial_number=serial_number, path=fds.path)), "'send' failed"
    check = fds.check(serial_number=serial_number)
    receive = fds.receive(serial_number=serial_number)
    assert check['id'] == sender and check['_id'] == serial_number, "'check' failed"
    assert receive == item, "'receive' error"
    fds.cancel(serial_number=serial_number)
    check2 = fds.check(serial_number=serial_number)
    assert not check2, "'cancel' error"


def test_mongo_delivery_service(fake_mongodb):
    db = 'test'
    collection = 'cache'
    mds = MongoDeliveryService(obj=Delivery, mongodb=fake_mongodb, db=db, collection=collection)
    sender = 'sender'
    serial_number = '1234'
    item = 'Hello, World!'
    mds.send(id=sender, serial_number=serial_number, item=item)
    assert fake_mongodb[db][collection].find_one({'_id': serial_number}), "'send' failed"
    check = mds.check(serial_number=serial_number)
    receive = mds.receive(serial_number=serial_number)
    assert check['id'] == sender and check['_id'] == serial_number, "'check' failed"
    assert receive == item, "'receive' error"
    mds.cancel(serial_number=serial_number)
    check2 = mds.check(serial_number=serial_number)
    assert not check2, "'cancel' error"
