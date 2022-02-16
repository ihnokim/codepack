import os
from codepack.storage import FileStorage
from codepack.service.snapshot_service import SnapshotService
from collections.abc import Iterable
from glob import glob


class FileSnapshotService(SnapshotService, FileStorage):
    def __init__(self, item_type=None, path='./'):
        FileStorage.__init__(self, item_type=item_type, path=path)

    def save(self, snapshot):
        if isinstance(snapshot, self.item_type):
            d = self.load(snapshot.serial_number)
            if d:
                existing_snapshot = self.item_type.from_dict(d)
                for key, value in existing_snapshot.diff(snapshot).items():
                    existing_snapshot[key] = value
                    existing_snapshot.to_file(self.item_type.get_path(serial_number=existing_snapshot.serial_number, path=self.path))
            else:
                snapshot.to_file(self.item_type.get_path(serial_number=snapshot.serial_number, path=self.path))
        else:
            raise TypeError(type(snapshot))

    def load(self, serial_number, projection=None):
        if isinstance(serial_number, str):
            ret = None
            try:
                d = self.item_type.from_file(self.item_type.get_path(serial_number=serial_number, path=self.path)).to_dict()
                if projection:
                    ret = {k: d[k] for k in set(projection).union({'serial_number'})}
                else:
                    ret = d
            finally:
                return ret
        elif isinstance(serial_number, Iterable):
            ret = list()
            for s in serial_number:
                tmp = self.load(serial_number=s, projection=projection)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def remove(self, serial_number):
        os.remove(self.item_type.get_path(serial_number=serial_number, path=self.path))

    def search(self, key, value, projection=None):
        ret = list()
        for filename in glob(self.path + '*.json'):
            snapshot = self.item_type.from_file(filename)
            if snapshot[key] != value:
                continue
            d = snapshot.to_dict()
            if projection:
                ret.append({k: d[k] for k in set(projection).union({'serial_number'})})
            else:
                ret.append(d)
        return ret
