from codepack import Delivery


def test_delivery_timestamp():
    delivery1 = Delivery(id='test@0.1.1', serial_number='12345678', item=1)
    d = delivery1.to_dict()
    assert '_timestamp' in d
    delivery2 = Delivery.from_dict(d)
    assert delivery2.get_timestamp() == d['_timestamp']
