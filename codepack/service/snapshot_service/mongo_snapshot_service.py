from codepack.storage import MongoStorage
from codepack.service.snapshot_service import SnapshotService
from collections.abc import Iterable
from codepack.utils.state import State


class MongoSnapshotService(SnapshotService, MongoStorage):
    def __init__(self, obj=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        MongoStorage.__init__(self, obj=obj, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def save(self, snapshot):
        if isinstance(snapshot, self.obj):
            d = self.load(snapshot.serial_number)
            if d:
                existing_snapshot = self.obj.from_dict(d)
                diff = existing_snapshot.diff(snapshot)
                for key, value in diff.items():
                    existing_snapshot[key] = value
                if 'state' in diff and isinstance(diff['state'], State):
                    diff['state'] = diff['state'].name
                self.mongodb[self.db][self.collection].update_one({'_id': existing_snapshot.serial_number},
                                                                  {'$set': diff}, upsert=True)
            else:
                snapshot.to_db(mongodb=self.mongodb, db=self.db, collection=self.collection)
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
            return self.mongodb[self.db][self.collection].find_one({'_id': serial_number}, projection=_projection)
        elif isinstance(serial_number, Iterable):
            return list(self.mongodb[self.db][self.collection].find({'_id': {'$in': serial_number}}, projection=_projection))
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def remove(self, serial_number):
        self.mongodb[self.db][self.collection].delete_one({'_id': serial_number})

    def search(self, key, value, projection=None):
        if projection:
            _projection = {k: True for k in projection}
            _projection['serial_number'] = True
            _projection['_id'] = False
        else:
            _projection = projection
        return list(self.mongodb[self.db][self.collection].find({key: value}, projection=_projection))
