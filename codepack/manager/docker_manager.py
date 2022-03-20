from codepack.manager.manager import Manager
from codepack.config import Config
import os
from docker.errors import ImageNotFound
from shutil import rmtree
from typing import Union
from docker import DockerClient
from codepack.interface import Docker
import json


class DockerManager(Manager):
    CONTAINER_WORK_DIR = '/usr/src/codepack'
    CONTAINER_LOG_DIR = '/usr/logs'

    def __init__(self, docker: Union[Docker, DockerClient, dict] = None, path: str = './', run_opt: str = None):
        if docker is None:
            self.docker = Docker()
        elif isinstance(docker, dict):
            self.docker = Docker(config=docker)
        else:
            self.docker = docker
        self.path = path
        if run_opt:
            self.run_opt = json.loads(run_opt)
        else:
            self.run_opt = dict()

    def get_image(self, image: str):
        ret = None
        try:
            ret = self.docker.images.get(image)
        except ImageNotFound:
            ret = self.docker.images.pull(image)
        finally:
            return ret

    def pull_image(self, image: str):
        return self.docker.images.pull(image)

    def push_image(self, image: str):
        return self.docker.images.push(image)

    def run(self, image: str, command: Union[str, list] = None, path: str = None,
            volumes: list = None, environment: list = None, **kwargs):
        if volumes is None:
            volumes = list()
        if environment is None:
            environment = list()
        _path = path if path else self.path
        return self.docker.containers\
            .run(image=image, command=command,
                 volumes=volumes + ['%s:%s' % (os.path.abspath(_path), self.CONTAINER_WORK_DIR),
                                    '%s:%s' % (os.path.abspath(Config.get_log_dir()), self.CONTAINER_LOG_DIR)],
                 environment=environment + ['%s=%s' % (Config.LABEL_LOGGER_LOG_DIR, self.CONTAINER_LOG_DIR)],
                 working_dir=self.CONTAINER_WORK_DIR, auto_remove=True, name=id(self),
                 **self.run_opt, **kwargs)

    @staticmethod
    def extract_requirements_from_file(path: str):
        with open(path, 'r') as f:
            lines = f.readlines()
        return [line.strip() for line in lines]

    @staticmethod
    def _collect_requirements_in_dict(r: list, d: dict):
        for _r in r:
            tokens = _r.split('==')
            if len(tokens) == 2:
                d[tokens[0]] = tokens[1]
            elif len(tokens) == 1:
                d[tokens[0]] = None
            else:
                raise IndexError(len(tokens))

    @staticmethod
    def combine_requirements(base: list, additive: list):
        ret = list()
        buffer = dict()
        DockerManager._collect_requirements_in_dict(base, buffer)
        DockerManager._collect_requirements_in_dict(additive, buffer)
        for module, version in buffer.items():
            tmp = module
            if version:
                tmp += '==%s' % version
            ret.append(tmp)
        return ret

    @staticmethod
    def make_dockerfile(base_image: str, path: str = './', args: dict = None, envs: dict = None,
                        requirements: list = None, pip_options: dict = None):
        ret = str()
        if args is None:
            args = dict()
        if envs is None:
            envs = dict()
        if requirements is None:
            requirements = list()
        lines = list()
        lines.append('FROM %s' % base_image)
        for k, v in args.items():
            lines.append('ARG %s %s' % (k, v))
        for k, v in envs.items():
            lines.append('ENV %s %s' % (k, v))
        if len(requirements) > 0:
            prefix = 'RUN python -m pip install'
            if pip_options:
                for k, v in pip_options.items():
                    if v:
                        if isinstance(v, list):
                            for _v in v:
                                prefix += ' --%s %s' % (k, _v)
                        elif isinstance(v, str):
                            prefix += ' --%s %s' % (k, v)
                        else:
                            raise TypeError(type(v))
                    else:
                        prefix += ' --%s' % k
            lines.append('%s pip' % prefix)
            lines.append('%s %s' % (prefix, ' '.join(requirements)))
        with open(os.path.join(path, 'Dockerfile'), 'w') as f:
            f.writelines(line + '\n' for line in lines)
        for line in lines:
            ret += line + '\n'
        return ret

    def build_image(self, tag, base_image, args=None, envs=None, requirements=None, pip_options=None,
                    tmp_dir: str = None, path: str = None):
        if not tmp_dir:
            tmp_dir = str(id(self))
        _path = os.path.join(path if path else self.path, tmp_dir)
        new_path = False
        image = None
        logs = None
        try:
            if self.get_image(base_image) is None:
                return None, None
            if not os.path.exists(_path):
                os.makedirs(_path)
                new_path = True
            self.make_dockerfile(base_image=base_image, path=_path, args=args, envs=envs,
                                 requirements=requirements, pip_options=pip_options)
            image, logs = self.docker.images.build(path=_path, tag=tag)
        finally:
            if new_path:
                rmtree(_path)
            else:
                self.remove_file_if_exists(os.path.join(_path, 'requirements.txt'))
                self.remove_file_if_exists(os.path.join(_path, 'Dockerfile'))
            return image, logs

    @staticmethod
    def remove_file_if_exists(path: str):
        if os.path.exists(path):
            os.remove(path)
