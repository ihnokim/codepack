from typing import Any, Dict, List
from codepack.interfaces.interface import Interface
from copy import deepcopy
import os
from shutil import rmtree
from glob import glob


class FileInterface(Interface):
    def __init__(self, path: str) -> None:
        self.path = path
        self._is_closed = False
        self.connect()

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return deepcopy(config)

    def connect(self) -> None:
        self.mkdir(path=self.path)

    def close(self) -> None:
        self._is_closed = True

    def is_closed(self) -> bool:
        return self._is_closed

    def get_session(self) -> 'FileInterface':
        return self

    @classmethod
    def mkdir(cls, path: str) -> None:
        if not cls.exists(path=path):
            os.makedirs(path)

    @classmethod
    def rmdir(cls, path: str) -> None:
        if cls.exists(path=path):
            rmtree(path)

    @classmethod
    def save_file(cls, dirname: str, filename: str, data: str) -> None:
        path = os.path.join(dirname, filename)
        with open(path, 'w+') as f:
            f.write(data)

    @classmethod
    def load_file(cls, dirname: str, filename: str) -> str:
        path = os.path.join(dirname, filename)
        with open(path, 'r') as f:
            data = f.read()
        return data

    @classmethod
    def remove_file(cls, dirname: str, filename: str) -> None:
        path = os.path.join(dirname, filename)
        os.remove(path=path)

    @classmethod
    def exists(cls, path: str) -> bool:
        return os.path.exists(path)

    @classmethod
    def listdir(cls, path: str) -> List[str]:
        return os.listdir(path)

    @classmethod
    def emptydir(cls, path: str) -> None:
        for item in glob(os.path.join(path, '*')):
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                rmtree(item)
            else:
                raise NotImplementedError('%s is unknown' % item)  # pragma: no cover
