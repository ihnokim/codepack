from codepack.storage import MemoryStorage
from codepack.service.snapshot_service import SnapshotService


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
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number):
        self.storage.remove(key=serial_number)

    def search(self, key, value, projection=None):
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)
