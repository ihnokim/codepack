from codepack.storages.storage import Storage
from typing import TypeVar, Type, Union, Optional, Any


Storable = TypeVar('Storable', bound='codepack.storages.storable.Storable')


class MemoryStorage(Storage):
    def __init__(self, item_type: Optional[Type[Storable]] = None, key: str = 'serial_number') -> None:
        super().__init__(item_type=item_type, key=key)
        self.memory = None
        self.init()

    def init(self) -> None:
        self.memory = dict()

    def close(self) -> None:
        self.memory.clear()
        self.memory = None

    def exist(self, key: Union[str, list], summary: str = '') -> Union[bool, list]:
        if isinstance(key, str):
            return key in self.memory.keys()
        elif isinstance(key, list):
            _summary, ret = self._validate_summary(summary=summary)
            for k in key:
                exists = k in self.memory.keys()
                if _summary == 'and' and not exists:
                    return False
                elif _summary == 'or' and exists:
                    return True
                elif _summary == '':
                    ret.append(exists)
            return ret
        else:
            raise TypeError(key)  # pragma: no cover

    def remove(self, key: Union[str, list]) -> None:
        if isinstance(key, str):
            self.memory.pop(key, None)
        elif isinstance(key, list):
            for k in key:
                self.remove(key=k)
        else:
            raise TypeError(key)  # pragma: no cover

    def search(self, key: str, value: Any, projection: Optional[list] = None, to_dict: bool = False) -> list:
        ret = list()
        for item in self.memory.values():
            d = item.to_dict()
            if d[key] != value:
                continue
            if projection:
                ret.append({k: d[k] for k in set(projection).union({self.key})})
            elif to_dict:
                ret.append(d)
            else:
                ret.append(item)
        return ret

    def list_all(self) -> list:
        return list(self.memory.keys())

    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        if isinstance(item, self.item_type):
            item_key = getattr(item, self.key)
            if not update and self.exist(key=item_key):
                raise ValueError('%s already exists' % item_key)
            else:
                self.memory[item_key] = item
        elif isinstance(item, list):
            for i in item:
                self.save(item=i, update=update)
        else:
            raise TypeError(item)  # pragma: no cover

    def update(self, key: Union[str, list], values: dict) -> None:
        if len(values) > 0:
            item = self.load(key=key, to_dict=True)
            if isinstance(item, dict):
                if item is not None:
                    for k, v in values.items():
                        item[k] = v
                    self.save(item=self.item_type.from_dict(item), update=True)
            elif isinstance(item, list):
                if len(item) > 0:
                    for i in item:
                        for k, v in values.items():
                            i[k] = v
                    self.save(item=[self.item_type.from_dict(i) for i in item], update=True)
            else:
                raise TypeError(type(item))  # pragma: no cover

    def load(self, key: Union[str, list], projection: Optional[list] = None, to_dict: bool = False)\
            -> Optional[Union[Storable, dict, list]]:
        if isinstance(key, str):
            if self.exist(key=key):
                item = self.memory[key]
                d = item.to_dict()
                if projection:
                    return {k: d[k] for k in set(projection).union({self.key})}
                elif to_dict:
                    return d
                else:
                    return item
            else:
                return None
        elif isinstance(key, list):
            ret = list()
            for s in key:
                tmp = self.load(key=s, projection=projection, to_dict=to_dict)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(key))  # pragma: no cover
