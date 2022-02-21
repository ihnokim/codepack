from codepack.storage import MongoStorage
from codepack.service.snapshot_service import SnapshotService
from collections.abc import Iterable
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
        if projection:
            _projection = {k: True for k in projection}
            _projection['serial_number'] = True
            _projection['_id'] = False
        else:
            _projection = projection
        if isinstance(serial_number, str):
            return self.storage.mongodb[self.storage.db][self.storage.collection]\
                .find_one({'_id': serial_number}, projection=_projection)
        elif isinstance(serial_number, Iterable):
            return list(self.storage.mongodb[self.storage.db][self.storage.collection]
                        .find({'_id': {'$in': serial_number}}, projection=_projection))
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def remove(self, serial_number):
        self.storage.remove(key=serial_number)

    def search(self, key, value, projection=None):
        if projection:
            _projection = {k: True for k in projection}
            _projection['serial_number'] = True
            _projection['_id'] = False
        else:
            _projection = projection
        return list(self.storage.mongodb[self.storage.db][self.storage.collection]
                    .find({key: value}, projection=_projection))
