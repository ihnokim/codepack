from codepack.storage import MongoStorage
from codepack.service.snapshot_service import SnapshotService
from codepack.snapshot import State


class MongoSnapshotService(SnapshotService):
    def __init__(self, item_type=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        self.storage = MongoStorage(item_type=item_type, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def save(self, snapshot):
        if isinstance(snapshot, self.storage.item_type):
            d = self.load(snapshot.serial_number)
            if d:
                existing_snapshot = self.storage.item_type.from_dict(d)
                diff = existing_snapshot.diff(snapshot)
                for key, value in diff.items():
                    existing_snapshot[key] = value
                if 'state' in diff and isinstance(diff['state'], State):
                    diff['state'] = diff['state'].name
                self.storage.mongodb[self.storage.db][self.storage.collection]\
                    .update_one({'_id': existing_snapshot.serial_number}, {'$set': diff}, upsert=True)
            else:
                snapshot.to_db(mongodb=self.storage.mongodb, db=self.storage.db, collection=self.storage.collection)
        else:
            raise TypeError(type(snapshot))

    def load(self, serial_number, projection=None):
        return self.storage.load(key=serial_number, projection=projection, to_dict=True)

    def remove(self, serial_number):
        self.storage.remove(key=serial_number)

    def search(self, key, value, projection=None):
        return self.storage.search(key=key, value=value, projection=projection, to_dict=True)
