from apps.apiserver.main import app
from fastapi.testclient import TestClient
from tests import *
from codepack import Code


test_client = TestClient(app)


def test_apiserver_save_and_load_and_delete_code():
    response = test_client.get('code/load/add2')
    assert response.status_code == 404
    assert response.json() == {'detail': 'add2 not found'}
    code = Code(add2)
    response = test_client.post('code/save', json={'code': code.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
    response = test_client.get('code/load/add2')
    assert response.status_code == 200
    response_json = response.json()
    code2 = Code.from_dict(response_json)
    assert code.get_name() == code2.get_name()
    assert code.description == code2.description
    assert code2.function(1, 3) == 4
    response = test_client.delete('code/remove/add2')
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
    response = test_client.get('code/load/add2')
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {'detail': 'add2 not found'}


def test_apiserver_search_code():
    response = test_client.get('code/load/add2')
    assert response.status_code == 404
    assert response.json() == {'detail': 'add2 not found'}
    response = test_client.get('code/load/add3')
    assert response.status_code == 404
    assert response.json() == {'detail': 'add3 not found'}
    response = test_client.get('code/search', json={'query': 'add'})
    assert response.status_code == 200
    assert response.json() == []
    code1 = Code(add2)
    code2 = Code(add3, version='0.0.1')
    response = test_client.post('code/save', json={'code': code1.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
    response = test_client.post('code/save', json={'code': code2.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'add3@0.0.1'}
    response = test_client.get('code/search', json={'query': 'add'})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 2
    assert sorted([x['_id'] for x in response_json]) == ['add2', 'add3@0.0.1']
    for item in response_json:
        assert item.keys() == {'_name', '_timestamp', '_id', 'source',
                               'description', 'env', 'image', 'owner', 'context'}
    response = test_client.get('code/search', json={'query': 'add', 'projection': ['_name', 'owner']})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 2
    for item in response_json:
        assert item.keys() == {'_name', 'owner'}
    response = test_client.get('code/search', json={'query': 'add3'})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 1
    assert response_json[0]['_name'] == 'add3@0.0.1'
    response = test_client.delete('code/remove/add2')
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
    response = test_client.delete('code/remove/add3@0.0.1')
    assert response.status_code == 200
    assert response.json() == {'name': 'add3@0.0.1'}
    response = test_client.get('code/search', json={'query': 'add'})
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 0


def test_apiserver_update_code():
    code = Code(add2, owner='codepack')
    response = test_client.post('code/save', json={'code': code.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
    response = test_client.get('code/load/add2')
    assert response.status_code == 200
    response_json = response.json()
    assert response_json.keys() == {'_name', '_timestamp', '_id', 'source',
                                    'description', 'env', 'image', 'owner', 'context'}
    assert response_json['owner'] == 'codepack'
    code.owner = 'test_user'
    response = test_client.post('code/save', json={'code': code.to_dict()})
    assert response.status_code == 409
    assert response.json() == {'detail': 'add2 already exists'}
    response = test_client.patch('code/update', json={'code': code.to_dict()})
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
    response = test_client.get('code/load/add2')
    assert response.status_code == 200
    response_json = response.json()
    assert response_json.keys() == {'_name', '_timestamp', '_id', 'source',
                                    'description', 'env', 'image', 'owner', 'context'}
    assert response_json['owner'] == 'test_user'
    response = test_client.delete('code/remove/add2')
    assert response.status_code == 200
    assert response.json() == {'name': 'add2'}
