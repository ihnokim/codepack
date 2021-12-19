from codepack.service import MemoryDeliveryService, FileDeliveryService, MongoDeliveryService
from codepack.utils.order import Order
import os


def test_singleton_memory_delivery_service():
    mds1 = MemoryDeliveryService()
    mds1.init()
    sender = 'sender'
    invoice_number = '1234'
    item = 'Hello, World!'
    mds1.send(sender=sender, invoice_number=invoice_number, item=item)
    mds2 = MemoryDeliveryService()
    assert mds1 == mds2
    assert len(mds2.deliveries) == 1
    assert mds2.check(invoice_number=invoice_number)


def test_memory_delivery_service_check():
    mds = MemoryDeliveryService()
    mds.init()
    sender = 'sender'
    invoice_number = '1234'
    item = 'Hello, World!'
    mds.send(sender=sender, invoice_number=invoice_number, item=item)
    assert isinstance(mds.check(invoice_number=invoice_number), dict)
    mds.cancel(invoice_number=invoice_number)
    invoice_numbers = ['123', '456', '789']
    mds.send(sender=sender, invoice_number=invoice_numbers[0], item=item)
    mds.send(sender=sender, invoice_number=invoice_numbers[1], item=item)
    check = mds.check(invoice_number=invoice_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_file_delivery_service_check(testdir_delivery_service):
    fds = FileDeliveryService(path=testdir_delivery_service)
    sender = 'sender'
    invoice_number = '1234'
    invoice_numbers = ['123', '456', '789']
    try:
        fds.cancel(invoice_number=invoice_number)
    except Exception:
        pass
    for i in invoice_numbers:
        try:
            fds.cancel(invoice_number=i)
        except Exception:
            continue
    item = 'Hello, World!'
    fds.send(sender=sender, invoice_number=invoice_number, item=item)
    assert isinstance(fds.check(invoice_number=invoice_number), dict)
    fds.cancel(invoice_number=invoice_number)
    fds.send(sender=sender, invoice_number=invoice_numbers[0], item=item)
    fds.send(sender=sender, invoice_number=invoice_numbers[1], item=item)
    check = fds.check(invoice_number=invoice_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_mongo_delivery_service_check(fake_mongodb):
    db = 'test'
    collection = 'cache'
    mds = MongoDeliveryService(mongodb=fake_mongodb, db=db, collection=collection)
    sender = 'sender'
    invoice_number = '1234'
    invoice_numbers = ['123', '456', '789']
    mds.cancel(invoice_number=invoice_number)
    for i in invoice_numbers:
        mds.cancel(invoice_number=i)
    item = 'Hello, World!'
    mds.send(sender=sender, invoice_number=invoice_number, item=item)
    assert isinstance(mds.check(invoice_number=invoice_number), dict)
    mds.cancel(invoice_number=invoice_number)
    mds.send(sender=sender, invoice_number=invoice_numbers[0], item=item)
    mds.send(sender=sender, invoice_number=invoice_numbers[1], item=item)
    check = mds.check(invoice_number=invoice_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_memory_delivery_service():
    mds = MemoryDeliveryService()
    sender = 'sender'
    invoice_number = '1234'
    item = 'Hello, World!'
    mds.send(sender=sender, invoice_number=invoice_number, item=item)
    assert invoice_number in mds.deliveries, "'send' failed"
    check = mds.check(invoice_number=invoice_number)
    receive = mds.receive(invoice_number=invoice_number)
    assert check['sender'] == sender and check['_id'] == invoice_number, "'check' failed"
    assert receive == item, "'receive' failed"
    mds.cancel(invoice_number=invoice_number)
    check2 = mds.check(invoice_number=invoice_number)
    assert not check2, "'cancel' failed"


def test_file_delivery_service(testdir_delivery_service):
    fds = FileDeliveryService(path=testdir_delivery_service)
    sender = 'sender'
    invoice_number = '1234'
    item = 'Hello, World!'
    fds.send(sender=sender, invoice_number=invoice_number, item=item)
    assert os.path.isfile(Order.get_path(serial_number=invoice_number, path=fds.path)), "'send' failed"
    check = fds.check(invoice_number=invoice_number)
    receive = fds.receive(invoice_number=invoice_number)
    assert check['sender'] == sender and check['_id'] == invoice_number, "'check' failed"
    assert receive == item, "'receive' error"
    fds.cancel(invoice_number=invoice_number)
    check2 = fds.check(invoice_number=invoice_number)
    assert not check2, "'cancel' error"


def test_mongo_delivery_service(fake_mongodb):
    db = 'test'
    collection = 'cache'
    mds = MongoDeliveryService(mongodb=fake_mongodb, db=db, collection=collection)
    sender = 'sender'
    invoice_number = '1234'
    item = 'Hello, World!'
    mds.send(sender=sender, invoice_number=invoice_number, item=item)
    assert fake_mongodb[db][collection].find_one({'_id': invoice_number}), "'send' failed"
    check = mds.check(invoice_number=invoice_number)
    receive = mds.receive(invoice_number=invoice_number)
    assert check['sender'] == sender and check['_id'] == invoice_number, "'check' failed"
    assert receive == item, "'receive' error"
    mds.cancel(invoice_number=invoice_number)
    check2 = mds.check(invoice_number=invoice_number)
    assert not check2, "'cancel' error"
