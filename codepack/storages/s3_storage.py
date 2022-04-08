from codepack.interfaces.s3 import S3
from codepack.storages.storage import Storage
from codepack.storages.storable import Storable
from typing import Type, Union, Optional, Any
from posixpath import join
import os


class S3Storage(Storage):
    def __init__(self, item_type: Optional[Type[Storable]] = None, key: str = 'serial_number',
                 s3: Optional[Union[S3, dict]] = None,
                 bucket: Optional[str] = None, path: str = '', *args: Any, **kwargs: Any) -> None:
        super().__init__(item_type=item_type, key=key)
        self.s3 = None
        self.bucket = None
        self.path = None
        self.new_connection = None
        self.init(s3=s3, bucket=bucket, path=path, *args, **kwargs)

    def init(self, s3: Optional[Union[S3, dict]] = None,
             bucket: Optional[str] = None, path: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        self.bucket = bucket
        self.path = path
        if isinstance(s3, S3):
            self.s3 = s3
            self.new_connection = False
        elif isinstance(s3, dict):
            self.s3 = S3(s3, *args, **kwargs)
            self.new_connection = True
        else:
            raise TypeError(type(s3))  # pragma: no cover

    def close(self) -> None:
        if self.new_connection:
            self.s3.close()
        self.s3 = None

    def exist(self, key: Union[str, list], summary: str = '') -> Union[bool, list]:
        if isinstance(key, str):
            path = self.item_type.get_path(key=key, path=self.path, posix=True)
            return self.s3.exist(bucket=self.bucket, key=path)
        elif isinstance(key, list):
            _summary, ret = self._validate_summary(summary=summary)
            for k in key:
                exists = self.exist(key=k, summary=summary)
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
            path = self.item_type.get_path(key=key, path=self.path, posix=True)
            self.s3.delete(bucket=self.bucket, key=path)
        elif isinstance(key, list):
            for k in key:
                self.remove(key=k)
        else:
            raise TypeError(key)  # pragma: no cover

    def search(self, key: str, value: Any, projection: Optional[list] = None, to_dict: bool = False) -> list:
        ret = list()
        if projection:
            to_dict = True
        all_obj_info = self.s3.list_objects(bucket=self.bucket, prefix=join(self.path, ''))
        all_obj_keys = [obj['Key'] for obj in all_obj_info]
        for k in all_obj_keys:
            try:
                instance = self.item_type.from_json(self.s3.download(bucket=self.bucket, key=k))
                d = instance.to_dict()
                if d[key] == value:
                    if projection:
                        ret.append({k: d[k] for k in set(projection).union({self.key})})
                    elif to_dict:
                        ret.append(d)
                    else:
                        ret.append(instance)
            except Exception:
                continue
        return ret

    def list_all(self) -> list:
        all_obj_info = self.s3.list_objects(bucket=self.bucket, prefix=join(self.path, ''))
        return [os.path.basename(obj['Key']).replace('.json', '') for obj in all_obj_info]

    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        if isinstance(item, self.item_type):
            item_key = getattr(item, self.key)
            path = item.get_path(key=item_key, path=self.path, posix=True)
            if update:
                if self.exist(key=item_key):
                    self.remove(key=item_key)
                self.s3.upload(bucket=self.bucket, key=path, data=item.to_json())
            elif self.exist(key=item_key):
                raise ValueError('%s already exists' % item_key)
            else:
                self.s3.upload(bucket=self.bucket, key=path, data=item.to_json())
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
            path = self.item_type.get_path(key=key, path=self.path, posix=True)
            ret_json = self.s3.download(bucket=self.bucket, key=path)
            if ret_json is None:
                return None
            ret_instance = self.item_type.from_json(ret_json)
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
