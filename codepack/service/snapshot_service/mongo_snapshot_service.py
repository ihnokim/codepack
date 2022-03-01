from codepack.storage import MongoStorage
from codepack.service.snapshot_service import SnapshotService
from codepack.snapshot import State
from codepack.snapshot import Snapshot
from typing import Type


class MongoSnapshotService(SnapshotService):
    def __init__(self, item_type: Type[Snapshot] = None, mongodb=None, db=None, collection=None, *args, **kwargs):
        self.storage = MongoStorage(item_type=item_type, key='serial_number',
                                    mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def save(self, snapshot: Snapshot):
        if self.storage.exist(key=snapshot.serial_number):
            existing_snapshot = self.storage.load(key=snapshot.serial_number)
            self.storage.update(key=snapshot.serial_number, values=existing_snapshot.diff(snapshot))
        else:
            self.storage.save(item=snapshot)

    def load(self, serial_number, projection=None):
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number):
        self.storage.remove(key=serial_number)

    def search(self, key, value, projection=None):
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)
