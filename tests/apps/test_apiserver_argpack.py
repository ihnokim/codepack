from tests import *
from codepack import Code, CodePack, ArgPack


def test_apiserver_save_and_load_and_delete_argpack(test_client):
    response = test_client.get('argpack/load/test_argpack')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_argpack not found'}
    code1 = Code(add2)
    code2 = Code(mul2)
    code2.receive('b') << code1
    codepack = CodePack(name='test_argpack', code=code1 >> code2, subscribe=code2)
    argpack = codepack.make_argpack()
    argpack['add2'](a=3, b=5)
    argpack['mul2'](a=2)
    response = test_client.post('argpack/save', json={'argpack': argpack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack'}
    response = test_client.get('argpack/load/test_argpack')
    assert response.status_code == 200
    response_json = response.json()
    argpack2 = ArgPack.from_dict(response_json)
    assert argpack.get_name() == argpack2.get_name()
    response = test_client.delete('argpack/remove/test_argpack')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack'}
    response = test_client.get('argpack/load/test_argpack')
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {'detail': 'test_argpack not found'}


def test_apiserver_save_and_load_argpack_dict(test_client):
    response = test_client.get('argpack/load/test_argpack@0.0.1')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_argpack@0.0.1 not found'}
    argpack = {'args': {'add2': {'a': 3, 'b': 5}, 'mul2': {'a': 2}}, '_name': 'test_argpack@0.0.1'}
    response = test_client.post('argpack/save', json={'argpack': argpack})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack@0.0.1'}
    response = test_client.get('argpack/load/test_argpack@0.0.1')
    assert response.status_code == 200
    response_json = response.json()
    argpack2 = ArgPack.from_dict(response_json)
    assert argpack2.get_name() == 'test_argpack@0.0.1'
    assert argpack2.get_version() == '0.0.1'
    response = test_client.delete('argpack/remove/test_argpack@0.0.1')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack@0.0.1'}
    response = test_client.get('argpack/load/test_argpack@0.0.1')
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {'detail': 'test_argpack@0.0.1 not found'}


def test_apiserver_save_anonymous_argpack_dict(test_client):
    argpack = {'args': {'add2': {'a': 3, 'b': 5}, 'mul2': {'a': 2}}}
    response = test_client.post('argpack/save', json={'argpack': argpack})
    assert response.status_code == 422
    assert response.json() == {'detail': 'name should not be null'}


def test_apiserver_search_argpack(test_client):
    response = test_client.get('argpack/load/test_argpack1')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_argpack1 not found'}
    response = test_client.get('argpack/load/test_argpack2')
    assert response.status_code == 404
    assert response.json() == {'detail': 'test_argpack2 not found'}
    response = test_client.get('argpack/search/test')
    assert response.status_code == 200
    assert response.json() == []
    argpack1 = ArgPack(name='test_argpack1', args={'add2': {'a': 3, 'b': 5}, 'mul2': {'b': 4}})
    argpack2 = ArgPack(name='test_argpack2', version='0.2.1', args={'add2': {'b': 5}, 'mul2': {'a': 3, 'b': 4}})
    response = test_client.post('argpack/save', json={'argpack': argpack1.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack1'}
    response = test_client.post('argpack/save', json={'argpack': argpack2.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack2@0.2.1'}
    response = test_client.get('argpack/search/test_argpack')
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 2
    assert sorted([x['_id'] for x in response_json]) == ['test_argpack1', 'test_argpack2@0.2.1']
    for item in response_json:
        assert item.keys() == {'_name', '_timestamp', '_id', 'args', 'owner'}
    response = test_client.get('argpack/search/test_argpack2')
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 1
    assert response_json[0]['_name'] == 'test_argpack2@0.2.1'
    response = test_client.delete('argpack/remove/test_argpack1')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack1'}
    response = test_client.delete('argpack/remove/test_argpack2@0.2.1')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack2@0.2.1'}
    response = test_client.get('argpack/search/test_argpack')
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 0


def test_apiserver_update_argpack(test_client):
    argpack = ArgPack(name='test_argpack', args={'add2': {'a': 3, 'b': 5}, 'mul2': {'b': 4}}, owner='codepack')
    response = test_client.post('argpack/save', json={'argpack': argpack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack'}
    response = test_client.get('argpack/load/test_argpack')
    assert response.status_code == 200
    response_json = response.json()
    assert response_json.keys() == {'_name', '_timestamp', '_id', 'args', 'owner'}
    assert response_json['owner'] == 'codepack'
    argpack.owner = 'test_user'
    response = test_client.post('argpack/save', json={'argpack': argpack.to_dict()})
    assert response.status_code == 409
    assert response.json() == {'detail': 'test_argpack already exists'}
    response = test_client.patch('argpack/update', json={'argpack': argpack.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack'}
    response = test_client.get('argpack/load/test_argpack')
    assert response.status_code == 200
    response_json = response.json()
    assert response_json.keys() == {'_name', '_timestamp', '_id', 'owner', 'args'}
    assert response_json['owner'] == 'test_user'
    response = test_client.delete('argpack/remove/test_argpack')
    assert response.status_code == 200
    assert response.json() == {'name': 'test_argpack'}
