from codepack.docker import DockerManager


def test_make_dockerfile(testdir_docker_manager):
    ret = DockerManager.make_dockerfile(path='testdir/docker_test', base_image='python:3.7-slim',
                                        args={'http_proxy': "http://1.2.3.4:5678", 'https_proxy': "''"},
                                        requirements=['pymongo==3.12.1', 'numpy'],
                                        pip_options={'upgrade': None,
                                                     'trusted-host': ['pypi.python.org', 'pypi.org', 'files.pythonhosted.org']})
    assert ret == "FROM python:3.7-slim\nARG http_proxy http://1.2.3.4:5678\nARG https_proxy ''\nRUN python -m " \
                  "pip install --upgrade --trusted-host pypi.python.org --trusted-host pypi.org " \
                  "--trusted-host files.pythonhosted.org pip\nRUN python -m pip install --upgrade " \
                  "--trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org " \
                  "pymongo==3.12.1 numpy\n"


def test_combine_requirements():
    ret = DockerManager.combine_requirements(base=['aaa', 'bbb==2.3', 'ccc'], additive=['bbb', 'ccc==2.5'])
    assert sorted(ret) == sorted(['aaa', 'bbb', 'ccc==2.5'])
