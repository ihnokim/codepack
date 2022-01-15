import os
from docker import from_env
from docker.errors import ImageNotFound
from shutil import rmtree


class DockerManager:
    def __init__(self, docker, path='./'):
        self.docker = docker
        self.path = path

    def get_image(self, image):
        self.docker = from_env()
        ret = None
        try:
            ret = self.docker.images.get(image)
        except ImageNotFound:
            ret = self.docker.images.pull(image)
        finally:
            return ret

    def pull_image(self, image):
        return self.docker.images.pull(image)

    def push_image(self, image):
        return self.docker.images.push(image)

    def run(self, image, command=None):
        return self.docker.containers.run(image=image, volumes=['%s:/usr/src/codepack' % os.path.abspath(self.path)],
                                          working_dir='/usr/src/codepack', auto_remove=True, name=id(self),
                                          command=command)

    def build_image(self, tag, base_image, args=None, envs=None, requirements=None, tmp_dir=None):
        if not tmp_dir:
            tmp_dir = str(id(self))
        path = os.path.join(self.path, tmp_dir)
        new_path = False
        image = None
        logs = None
        try:
            if self.get_image(base_image) is None:
                return None, None
            if args is None:
                args = dict()
            if envs is None:
                envs = dict()
            if not os.path.exists(path):
                os.makedirs(path)
                new_path = True
            if requirements is None:
                requirements = list()
            if len(requirements) > 0:
                with open(os.path.join(path, 'requirements.txt'), 'w') as f:
                    lines = list()
                    lines.append('FROM %s' % base_image)
                    lines.append('COPY requirements.txt requirements.txt')
                    for k, v in args.items():
                        lines.append('ARG %s %s' % (k, v))
                    for k, v in envs.items():
                        lines.append('ENV %s %s' % (k, v))
                    if len(requirements) > 0:
                        prefix = 'RUN python -m pip install --upgrade --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org'
                        lines.append('%s pip' % prefix)
                        lines.append('%s -r requirements.txt' % prefix)
                    f.writelines(line + '\n' for line in lines)
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
