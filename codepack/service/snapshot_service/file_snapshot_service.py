from codepack.storage import FileStorage
from codepack.service.snapshot_service import SnapshotService
from codepack.snapshot import Snapshot
from typing import Type


class FileSnapshotService(SnapshotService):
    def __init__(self, item_type: Type[Snapshot] = None, path='./'):
        self.storage = FileStorage(item_type=item_type, key='serial_number', path=path)

    def save(self, snapshot: Snapshot):
        if self.storage.exist(key=snapshot.serial_number):
            existing_snapshot = self.storage.load(key=snapshot.serial_number)
            self.storage.update(key=snapshot.serial_number, values=existing_snapshot.diff(snapshot))
        else:
            self.storage.save(item=snapshot)

    def load(self, serial_number: str, projection: list = None):
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number: str):
        self.storage.remove(key=serial_number)

    def search(self, key: str, value: object, projection: list = None):
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)
