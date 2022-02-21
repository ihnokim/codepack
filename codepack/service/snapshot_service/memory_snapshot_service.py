from codepack.storage import MemoryStorage
from codepack.service.snapshot_service import SnapshotService
from collections.abc import Iterable


class MemorySnapshotService(SnapshotService):
    def __init__(self, item_type=None):
        self.storage = MemoryStorage(item_type=item_type)

    def save(self, snapshot):
        if isinstance(snapshot, self.storage.item_type):
            d = self.load(snapshot.serial_number)
            if d:
                for key, value in self.storage.item_type.from_dict(d).diff(snapshot).items():
                    self.storage.memory[snapshot.serial_number][key] = value
            else:
                self.storage.memory[snapshot.serial_number] = snapshot
        else:
            raise TypeError(type(snapshot))

    def load(self, serial_number, projection=None):
        if isinstance(serial_number, str):
            if serial_number in self.storage.memory:
                d = self.storage.memory[serial_number].to_dict()
                if projection:
                    return {k: d[k] for k in set(projection).union({'serial_number'})}
                else:
                    return d
            else:
                return None
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
        self.storage.remove(key=serial_number)

    def search(self, key, value, projection=None):
        return self.storage.search(key=key, value=value, projection=projection)
