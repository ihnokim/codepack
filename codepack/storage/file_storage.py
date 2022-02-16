from codepack.storage import Storage, Storable
from shutil import rmtree
from glob import glob
import os
from typing import Type


class FileStorage(Storage):
    def __init__(self, item_type: Type[Storable] = None, path: str = '.'):
        super().__init__(item_type=item_type)
        self.path = None
        self.new_path = None
        self.init(path=path)

    def init(self, path: str = '.'):
        self.path = path
        if os.path.exists(path):
            self.new_path = False
        else:
            self.new_path = True
            self.mkdir(path)

    def close(self):
        if self.new_path:
            self.rmdir(self.path)

    @staticmethod
    def mkdir(path: str):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def rmdir(path: str):
        if os.path.exists(path):
            rmtree(path)

    @staticmethod
    def empty_dir(path: str):
        for item in glob(os.path.join(path, '*')):
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                rmtree(item)
            else:
                raise NotImplementedError('%s is unknown' % item)  # pragma: no cover
