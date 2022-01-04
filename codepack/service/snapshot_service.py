from codepack.utils.snapshot import Snapshot
from codepack.service.abc import SnapshotService
from collections.abc import Iterable
from codepack.utils import Singleton


class MemorySnapshotService(SnapshotService, Singleton):
    def __init__(self):
        SnapshotService.__init__(self)
        if not hasattr(self, 'memory'):
            self.memory = None
            self.init()

    def init(self):
        self.memory = dict()

    def save(self, snapshot):
        if isinstance(snapshot, Snapshot):
            loaded = self.load(snapshot.serial_number)
            if loaded:
                for key in snapshot.diff(loaded).keys():
                    self.memory[snapshot.serial_number][key] = snapshot[key]
            else:
                self.memory[snapshot.serial_number] = snapshot
        else:
            raise TypeError(type(snapshot))

    def load(self, serial_number, projection=None):
        if isinstance(serial_number, str):
            if serial_number in self.memory:
                d = self.memory[serial_number].to_dict()
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
        self.memory.pop(serial_number, None)

    def search(self, key, value, projection=None):
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