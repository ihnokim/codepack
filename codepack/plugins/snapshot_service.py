from codepack.plugins.service import Service
from codepack.plugins.snapshots.snapshot import Snapshot
from codepack.plugins.snapshots.snapshotable import Snapshotable
from codepack.plugins.state import State
from typing import Union, TypeVar, Optional, Any


Storage = TypeVar('Storage', bound='codepack.storages.storage.Storage')
Storable = TypeVar('Storable', bound='codepack.storages.storable.Storable')


class SnapshotService(Service):
    def __init__(self, storage: Storage) -> None:
        super().__init__(storage=storage)
        if self.storage.key != 'serial_number':
            self.storage.key = 'serial_number'

    def save(self, snapshot: Snapshot) -> None:
        if self.storage.exist(key=snapshot.serial_number):
            existing_snapshot = self.storage.load(key=snapshot.serial_number)
            diff = existing_snapshot.diff(snapshot)
            if 'state' in diff and isinstance(diff['state'], State):
                diff['state'] = diff['state'].name
            self.storage.update(key=snapshot.serial_number, values=diff)
        else:
            self.storage.save(item=snapshot)

    def load(self, serial_number: Union[str, list], projection: Optional[list] = None)\
            -> Optional[Union[Storable, dict, list]]:
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number: Union[str, list]):
        self.storage.remove(key=serial_number)

    def search(self, key: str, value: Any, projection: Optional[list] = None) -> list:
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)

    def convert_to_snapshot(self, item: Snapshotable, *args: Any, **kwargs: Any) -> Snapshot:
        return self.storage.item_type(item, *args, **kwargs)
