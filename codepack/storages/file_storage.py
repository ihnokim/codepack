from codepack.storages.storage import Storage
from typing import Dict, List, Optional, Any
from codepack.interfaces.file_interface import FileInterface
import json


class FileStorage(Storage):
    def __init__(self, path: str) -> None:
        super().__init__()
        self.path = path
        self.interface = FileInterface(path=path)

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return FileInterface.parse_config(config=config)

    @classmethod
    def get_filename(cls, id: id) -> str:
        return f'{id}.json'

    def save(self, id: str, item: Dict[str, Any]) -> bool:
        if not self.exists(id=id):
            self.interface.save_file(dirname=self.path,
                                     filename=self.get_filename(id=id),
                                     data=json.dumps(item))
            return True
        else:
            return False

    def load(self, id: str) -> Optional[Dict[str, Any]]:
        if self.exists(id=id):
            item_json = self.interface.load_file(dirname=self.path,
                                                 filename=self.get_filename(id=id))
            return json.loads(item_json)
        else:
            return None

    def update(self, id: str, **kwargs: Any) -> bool:
        if self.exists(id=id):
            d = self.load(id=id)
            for k, v in kwargs.items():
                d[k] = v
            self.interface.save_file(dirname=self.path,
                                     filename=self.get_filename(id=id),
                                     data=json.dumps(d))
            return True
        else:
            return False

    def remove(self, id: str) -> bool:
        if self.exists(id=id):
            self.interface.remove_file(dirname=self.path,
                                       filename=self.get_filename(id=id))
            return True
        else:
            return False

    def exists(self, id: str) -> bool:
        item_filename = self.get_filename(id=id)
        for filename in self.interface.listdir(self.path):
            if item_filename == filename:
                return True
        return False

    def search(self, key: str, value: Any) -> List[str]:
        ret = list()
        ids = self.list_all()
        for id in ids:
            d = self.load(id=id)
            if d[key] == value:
                ret.append(id)
        return ret

    def count(self, id: Optional[str] = None) -> int:
        if id:
            return len(self.list_like(id=id))
        else:
            return len(self.list_all())

    def list_all(self) -> List[str]:
        filenames = self.interface.listdir(path=self.path)
        return [filename.replace('.json', '') for filename in filenames if '.json' in filename]

    def list_like(self, id: str) -> List[str]:
        return [x for x in self.list_all() if id in x]

    def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if id:
            ret = list()
            for x in id:
                if self.exists(id=x):
                    ret.append(self.load(id=x))
            return ret
        else:
            return [self.load(x) for x in self.list_all()]

    def exists_many(self, id: List[str]) -> List[bool]:
        ids = set(self.list_all())
        ret = list()
        for i in id:
            ret.append(i in ids)
        return ret
