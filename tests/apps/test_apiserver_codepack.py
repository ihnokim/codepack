from tests import *
from codepack import Code, CodePack
import time


def test_apiserver_save_and_load_and_delete_codepack(test_client):
    response = test_client.get('codepack/load/test_codepack')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack not found'}
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack(name='test_codepack', code=code1 >> code2, subscribe=code2)
    response = test_client.post('codepack/save', json={'codepack': codepack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack'}
    response = test_client.get('codepack/load/test_codepack')
    assert response.status_code == 200
    response_json = response.json()
    codepack2 = CodePack.from_dict(response_json)
    assert codepack.get_name() == codepack2.get_name()
    argpack = codepack2.make_argpack()
    argpack['add2'](a=3, b=5)
    argpack['mul2'](a=2)
    assert codepack2(argpack) == 16
    response = test_client.delete('codepack/remove/test_codepack')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack'}
    response = test_client.get('codepack/load/test_codepack')
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {'detail': 'test_codepack not found'}


def test_apiserver_search_codepack(test_client):
    response = test_client.get('codepack/load/test_codepack1')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack1 not found'}
    response = test_client.get('codepack/load/test_codepack2')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack2 not found'}
    response = test_client.get('codepack/search', json={'query': 'test'})
    assert response.status_code == 200
    assert response.json() == []
    codepack1 = CodePack('test_codepack1', code=Code(add2) >> Code(mul2), subscribe='mul2')
    codepack2 = CodePack('test_codepack2', version='0.2.1', code=Code(mul2) >> Code(add2), subscribe='add2')
    response = test_client.post('codepack/save', json={'codepack': codepack1.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack1'}
    response = test_client.post('codepack/save', json={'codepack': codepack2.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack2@0.2.1'}
    response = test_client.get('codepack/search', json={'query': 'test_codepack'})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 2
    assert sorted([x['_id'] for x in response_json]) == ['test_codepack1', 'test_codepack2@0.2.1']
    for item in response_json:
        assert item.keys() == {'_name', '_timestamp', '_id', 'structure', 'owner', 'subscribe', 'source'}
    response = test_client.get('codepack/search', json={'query': 'test_codepack', 'projection': ['_name', 'structure']})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 2
    for item in response_json:
        assert item.keys() == {'_name', 'structure'}
    response = test_client.get('codepack/search', json={'query': 'test_codepack2'})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 1
    assert response_json[0]['_name'] == 'test_codepack2@0.2.1'
    response = test_client.delete('codepack/remove/test_codepack1')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack1'}
    response = test_client.delete('codepack/remove/test_codepack2@0.2.1')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack2@0.2.1'}
    response = test_client.get('codepack/search', json={'query': 'test_codepack'})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 0


def test_apiserver_update_codepack(test_client):
    codepack = CodePack('test_codepack', owner='codepack', code=Code(add2) >> Code(mul2), subscribe='mul2')
    response = test_client.post('codepack/save', json={'codepack': codepack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack'}
    response = test_client.get('codepack/load/test_codepack')
    assert response.status_code == 200
    response_json = response.json()
    assert response_json.keys() == {'_name', '_timestamp', '_id', 'source', 'structure', 'owner', 'subscribe'}
    assert response_json['owner'] == 'codepack'
    codepack.owner = 'test_user'
    response = test_client.post('codepack/save', json={'codepack': codepack.to_dict()})
    assert response.status_code == 409
    assert response.json() == {'detail': 'test_codepack already exists'}
    response = test_client.patch('codepack/update', json={'codepack': codepack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack'}
    response = test_client.get('codepack/load/test_codepack')
    assert response.status_code == 200
    response_json = response.json()
    assert response_json.keys() == {'_name', '_timestamp', '_id', 'source', 'structure', 'owner', 'subscribe'}
    assert response_json['owner'] == 'test_user'
    response = test_client.delete('codepack/remove/test_codepack')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack'}


def test_apiserver_run_codepack_without_worker(test_client):
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack('test_codepack', owner='codepack', code=code1 >> code2, subscribe=code2)
    argpack = {'args': {'mul2': {'a': 3}, 'add2': {'a': 2, 'b': 6}}}
    response = test_client.post('codepack/run', json={'codepack': codepack.to_dict(), 'argpack': argpack})
    assert response.status_code == 200
    response_json = response.json()
    serial_number = response_json['serial_number']
    response = test_client.get('codepack/state/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'state': 'READY'}


def test_apiserver_run_codepack_with_worker(test_client, test_worker):
    def test_callback(x):
        if x['state'] == 'TERMINATED':
            test_client.get('organize/%s' % x['_serial_number'])
    test_worker.callback = test_callback
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack('test_codepack', owner='codepack', code=code1 >> code2, subscribe=code2)
    argpack = {'args': {'mul2': {'a': 3}, 'add2': {'a': 2, 'b': 6}}}
    response = test_client.post('codepack/run', json={'codepack': codepack.to_dict(), 'argpack': argpack})
    assert response.status_code == 200
    response_json = response.json()
    serial_number = response_json['serial_number']
    time.sleep(3)
    response = test_client.get('codepack/state/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'state': 'TERMINATED'}
    response = test_client.get('codepack/result/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'result': 24}


def test_apiserver_run_codepack_by_name_with_worker(test_client, test_worker):
    def test_callback(x):
        if x['state'] == 'TERMINATED':
            test_client.get('organize/%s' % x['_serial_number'])
    test_worker.callback = test_callback
    response = test_client.get('codepack/load/test_codepack@0.0.2')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack@0.0.2 not found'}
    argpack = {'args': {'add2': {'a': 2, 'b': 5}, 'mul2': {'a': 3}}}
    response = test_client.post('codepack/run/test_codepack@0.0.2', json={'argpack': argpack})
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack@0.0.2 not found'}
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack('test_codepack', version='0.0.2', code=code1 >> code2, subscribe=code2)
    response = test_client.post('codepack/save', json={'codepack': codepack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack@0.0.2'}
    response = test_client.post('codepack/run/test_codepack@0.0.2', json={'argpack': argpack})
    assert response.status_code == 200
    response_json = response.json()
    serial_number = response_json['serial_number']
    time.sleep(3)
    response = test_client.get('codepack/state/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'state': 'TERMINATED'}
    response = test_client.get('codepack/result/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'result': 21}


def test_apiserver_run_codepack_by_name_pair_with_worker(test_client, test_worker):
    def test_callback(x):
        if x['state'] == 'TERMINATED':
            test_client.get('organize/%s' % x['_serial_number'])
    test_worker.callback = test_callback
    response = test_client.get('codepack/load/test_codepack@0.0.2')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack@0.0.2 not found'}
    argpack = {'_name': 'test_argpack', 'args': {'add2': {'a': 2, 'b': 5}, 'mul2': {'a': 3}}}
    response = test_client.post('codepack/run/test_codepack@0.0.2', json={'argpack': argpack})
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_codepack@0.0.2 not found'}
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack('test_codepack', version='0.0.2', code=code1 >> code2, subscribe=code2)
    response = test_client.post('codepack/save', json={'codepack': codepack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_codepack@0.0.2'}
    response = test_client.post('codepack/run/test_codepack@0.0.2/test_argpack')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_argpack not found'}
    response = test_client.post('argpack/save', json={'argpack': argpack})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack'}
    response = test_client.post('codepack/run/test_codepack@0.0.2/test_argpack')
    assert response.status_code == 200
    response_json = response.json()
    serial_number = response_json['serial_number']
    time.sleep(3)
    response = test_client.get('codepack/state/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'state': 'TERMINATED'}
    response = test_client.get('codepack/result/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'result': 21}


def test_apiserver_run_codepack_snapshot_with_worker(test_client, test_worker):
    def test_callback(x):
        if x['state'] == 'TERMINATED':
            test_client.get('organize/%s' % x['_serial_number'])
    test_worker.callback = test_callback
    argpack = {'_name': 'test_argpack', 'args': {'add2': {'a': 2, 'b': 5}, 'mul2': {'a': 3}}}
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack('test_codepack', version='0.0.2', code=code1 >> code2, subscribe=code2)
    snapshot = codepack.to_snapshot(argpack=argpack)
    response = test_client.post('codepack/snapshot/run', json={'snapshot': snapshot.to_dict()})
    assert response.status_code == 200
    response_json = response.json()
    serial_number = response_json['serial_number']
    time.sleep(3)
    response = test_client.get('codepack/state/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'state': 'TERMINATED'}
    response = test_client.get('codepack/result/%s' % serial_number)
    assert response.status_code == 200
    assert response.json() == {'serial_number': serial_number, 'result': 21}
