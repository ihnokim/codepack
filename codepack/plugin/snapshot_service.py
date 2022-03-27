from codepack.plugin.service import Service
from codepack.snapshot.snapshot import Snapshot
from codepack.snapshot.state import State
from codepack.snapshot.snapshotable import Snapshotable
from typing import Union


class SnapshotService(Service):
    def __init__(self, storage):
        super().__init__(storage=storage)
        if self.storage.key != 'serial_number':
            self.storage.key = 'serial_number'

    def save(self, snapshot: Snapshot):
        if self.storage.exist(key=snapshot.serial_number):
            existing_snapshot = self.storage.load(key=snapshot.serial_number)
            diff = existing_snapshot.diff(snapshot)
            if 'state' in diff and isinstance(diff['state'], State):
                diff['state'] = diff['state'].name
            self.storage.update(key=snapshot.serial_number, values=diff)
        else:
            self.storage.save(item=snapshot)

    def load(self, serial_number: Union[str, list], projection: list = None):
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number: Union[str, list]):
        self.storage.remove(key=serial_number)

    def search(self, key: str, value: object, projection: list = None):
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)

    def convert_to_snapshot(self, item: Snapshotable, *args, **kwargs):
        return self.storage.item_type(item, *args, **kwargs)
