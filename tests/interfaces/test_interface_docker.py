from unittest.mock import patch
from codepack.interfaces import Docker


@patch('docker.DockerClient')
def test_docker_init(mock_client):
    docker_config = {'base_url': 'unix://var/run/docker.sock'}
    d = Docker(config=docker_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('base_url', '') == 'unix://var/run/docker.sock'
    assert d.session is mock_client()
    d.close()
    mock_client().close.assert_called_once()
    assert d.closed()


@patch('docker.DockerClient')
def test_docker_attr(mock_client):
    docker_config = {'base_url': 'unix://var/run/docker.sock'}
    d = Docker(config=docker_config)
    d.images.get('test_image')
    mock_client().images.get.assert_called_once_with('test_image')
    d.close()
