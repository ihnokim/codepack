from codepack.storage import FileStorage
from codepack.service.snapshot_service import SnapshotService


class FileSnapshotService(SnapshotService):
    def __init__(self, item_type=None, path='./'):
        self.storage = FileStorage(item_type=item_type, path=path)

    def save(self, snapshot):
        if isinstance(snapshot, self.storage.item_type):
            d = self.load(snapshot.serial_number)
            if d:
                existing_snapshot = self.storage.item_type.from_dict(d)
                for key, value in existing_snapshot.diff(snapshot).items():
                    existing_snapshot[key] = value
                    existing_snapshot.to_file(self.storage.item_type.get_path(key=existing_snapshot.serial_number, path=self.storage.path))
            else:
                snapshot.to_file(self.storage.item_type.get_path(key=snapshot.serial_number, path=self.storage.path))
        else:
            raise TypeError(type(snapshot))

    def load(self, serial_number, projection=None):
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number):
        self.storage.remove(key=serial_number)

    def search(self, key, value, projection=None):
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)
