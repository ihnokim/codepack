from unittest.mock import patch
from codepack import DockerManager
from docker.errors import ImageNotFound


def test_make_dockerfile(testdir_docker_manager):
    ret = DockerManager.make_dockerfile(path='testdir/docker_test', base_image='python:3.7-slim',
                                        args={'http_proxy': "http://1.2.3.4:5678", 'https_proxy': "''"},
                                        requirements=['pymongo==3.12.1', 'numpy'],
                                        pip_options={'upgrade': None,
                                                     'trusted-host': ['pypi.python.org', 'pypi.org',
                                                                      'files.pythonhosted.org']})
    assert ret == "FROM python:3.7-slim\nARG http_proxy http://1.2.3.4:5678\nARG https_proxy ''\nRUN python -m " \
                  "pip install --upgrade --trusted-host pypi.python.org --trusted-host pypi.org " \
                  "--trusted-host files.pythonhosted.org pip\nRUN python -m pip install --upgrade " \
                  "--trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org " \
                  "pymongo==3.12.1 numpy\n"


def test_combine_requirements():
    ret = DockerManager.combine_requirements(base=['aaa', 'bbb==2.3', 'ccc'], additive=['bbb', 'ccc==2.5'])
    assert sorted(ret) == sorted(['aaa', 'bbb', 'ccc==2.5'])


def test_extract_requirements_from_file():
    requirements = DockerManager.extract_requirements_from_file('requirements.txt')
    assert requirements == ['dill==0.3.4', 'pymongo==3.12.1', 'sshtunnel==0.4.0', 'PyMySQL==1.0.2', 'pymssql==2.2.2',
                            'boto3==1.19.6', 'cx-Oracle==8.2.1', 'parse==1.19.0',
                            'APScheduler==3.8.1', 'kafka-python==2.0.2',
                            'docker==5.0.3', 'requests==2.26.0', 'numpy', 'pandas', 'pyreadline']


@patch('docker.DockerClient')
def test_docker_get_image(mock_client):
    docker_manager = DockerManager()
    docker_manager.get_image('dummy-image')
    mock_client.return_value.images.get.assert_called_once_with('dummy-image')
    mock_client.return_value.images.pull.assert_not_called()
    mock_client.return_value.images.get.side_effect = ImageNotFound('')
    docker_manager.get_image('dummy-image')
    assert mock_client.return_value.images.get.call_count == 2
    mock_client.return_value.images.pull.assert_called_once_with('dummy-image')
