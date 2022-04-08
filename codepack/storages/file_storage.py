from codepack.storages.storage import Storage
from codepack.storages.storable import Storable
from shutil import rmtree
from glob import glob
import os
from typing import Type, Union, Optional, Any


class FileStorage(Storage):
    def __init__(self, item_type: Optional[Type[Storable]] = None, key: str = 'serial_number', path: str = '.') -> None:
        super().__init__(item_type=item_type, key=key)
        self.path = None
        self.new_path = None
        self.init(path=path)

    def init(self, path: str = '.') -> None:
        self.path = path
        if os.path.exists(path):
            self.new_path = False
        else:
            self.new_path = True
            self.mkdir(path)

    def close(self) -> None:
        if self.new_path:
            self.rmdir(self.path)

    @staticmethod
    def mkdir(path: str) -> None:
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def rmdir(path: str) -> None:
        if os.path.exists(path):
            rmtree(path)

    @staticmethod
    def empty_dir(path: str) -> None:
        for item in glob(os.path.join(path, '*')):
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                rmtree(item)
            else:
                raise NotImplementedError('%s is unknown' % item)  # pragma: no cover

    def exist(self, key: Union[str, list], summary: str = '') -> Union[bool, list]:
        if isinstance(key, str):
            path = self.item_type.get_path(key=key, path=self.path)
            return os.path.exists(path)
        elif isinstance(key, list):
            _summary, ret = self._validate_summary(summary=summary)
            for k in key:
                path = self.item_type.get_path(key=k, path=self.path)
                exists = os.path.exists(path)
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
            os.remove(path=self.item_type.get_path(key=key, path=self.path))
        elif isinstance(key, list):
            for k in key:
                path = self.item_type.get_path(key=k, path=self.path)
                os.remove(path)
        else:
            raise TypeError(key)  # pragma: no cover

    def search(self, key: str, value: Any, projection: Optional[list] = None, to_dict: Optional[bool] = None) -> list:
        ret = list()
        for filename in glob(self.path + '*.json'):
            item = self.item_type.from_file(filename)
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
        return [filename.replace('.json', '') for filename in os.listdir(self.path)]

    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        if isinstance(item, self.item_type):
            item_key = getattr(item, self.key)
            path = item.get_path(key=item_key, path=self.path)
            if update:
                if self.exist(key=item_key):
                    self.remove(key=item_key)
                item.to_file(path)
            elif self.exist(key=item_key):
                raise ValueError('%s already exists' % item_key)
            else:
                item.to_file(path)
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
            if projection:
                to_dict = True
            path = self.item_type.get_path(key=key, path=self.path)
            if not self.exist(key=key):
                return None
            ret_instance = self.item_type.from_file(path)
            if projection:
                d = ret_instance.to_dict()
                return {k: d[k] for k in set(projection).union({self.key})}
            elif to_dict:
                return ret_instance.to_dict()
            else:
                return ret_instance
        elif isinstance(key, list):
            ret = list()
            for k in key:
                tmp = self.load(key=k, projection=projection, to_dict=to_dict)
                if tmp is not None:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(key)  # pragma: no cover
