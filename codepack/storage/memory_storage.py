from codepack.storage import Storage, Storable
from typing import Type, Union


class MemoryStorage(Storage):
    def __init__(self, item_type: Type[Storable] = None):
        super().__init__(item_type=item_type)
        self.memory = None
        self.init()

    def init(self):
        self.memory = dict()

    def close(self):
        self.memory.clear()
        self.memory = None

    def exist(self, key: Union[str, list]):
        if isinstance(key, str):
            return key in self.memory.keys()
        elif isinstance(key, list):
            for k in key:
                exists = k in self.memory.keys()
                if not exists:
                    return False
            return True
        else:
            raise TypeError(key)

    def remove(self, key: Union[str, list]):
        if isinstance(key, str):
            self.memory.pop(key, None)
        elif isinstance(key, list):
            for k in key:
                self.memory.pop(k, None)
        else:
            raise TypeError(key)

    def search(self, key: str, value: object, projection: list = None):
        ret = list()
        for snapshot in self.memory.values():
            if snapshot[key] != value:
                continue
            d = snapshot.to_dict()
            if projection:
                ret.append({k: d[k] for k in set(projection).union({'serial_number'})})
            else:
                ret.append(d)
        return ret
