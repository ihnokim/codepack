import os
from docker.errors import ImageNotFound
from shutil import rmtree
from typing import Union
from docker import DockerClient
from codepack.interface import Docker


class DockerManager:
    CONTAINER_WORKDIR = '/usr/src/codepack'

    def __init__(self, docker: Union[Docker, DockerClient] = None, path: str = './'):
        if docker is None:
            self.docker = Docker()
        else:
            self.docker = docker
        self.path = path

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

    def run(self, image: str, **kwargs):
        return self.docker.containers.run(image=image, volumes=['%s:%s' % (os.path.abspath(self.path), self.CONTAINER_WORKDIR)],
                                          working_dir=self.CONTAINER_WORKDIR, auto_remove=True, name=id(self), **kwargs)

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
    def make_dockerfile(path: str, base_image: str, args: dict = None, envs: dict = None,
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

    def build_image(self, tag, base_image, args=None, envs=None, requirements=None, pip_options=None, tmp_dir=None):
        if not tmp_dir:
            tmp_dir = str(id(self))
        path = os.path.join(self.path, tmp_dir)
        new_path = False
        image = None
        logs = None
        try:
            if self.get_image(base_image) is None:
                return None, None
            if not os.path.exists(path):
                os.makedirs(path)
                new_path = True
            self.make_dockerfile(path, base_image=base_image, args=args, envs=envs,
                                 requirements=requirements, pip_options=pip_options)
            image, logs = self.docker.images.build(path=path, tag=tag)
        finally:
            if new_path:
                rmtree(path)
            else:
                if os.path.exists(os.path.join(path, 'requirements.txt')):
                    os.remove(os.path.join(path, 'requirements.txt'))
                if os.path.exists(os.path.join(path, 'Dockerfile')):
                    os.remove(os.path.join(path, 'Dockerfile'))
            return image, logs
