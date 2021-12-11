from codepack.service.abc import StateManager
from codepack.service.mongodb_service import MongoDBService
from codepack.utils.state import State, StateCode
from collections.abc import Iterable
from codepack.utils import Singleton
import os


class MemoryStateManager(StateManager, Singleton):
    def __init__(self):
        super().__init__()
        if not hasattr(self, 'states'):
            self.states = dict()

    def init(self):
        self.states = dict()

    def set(self, id, serial_number, state, update_time=None, dependency=None):
        self.states[serial_number] = State(id=id, serial_number=serial_number,
                                           state=state, update_time=update_time, dependency=dependency)

    def get(self, serial_number):
        ret = StateCode.UNKNOWN
        try:
            ret = self.states[serial_number].state
        finally:
            return ret

    def check(self, serial_number):
        if isinstance(serial_number, str):
            if serial_number in self.states:
                return self.states[serial_number].to_dict()
            else:
                return None
        elif isinstance(serial_number, Iterable):
            ret = list()
            for s in serial_number:
                tmp = self.check(s)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def remove(self, serial_number):
        self.states.pop(serial_number, None)


class FileStateManager(StateManager):
    def __init__(self, path='./'):
        super().__init__()
        self.path = path

    def set(self, id, serial_number, state, update_time=None, dependency=None, path=None):
        State(id=id, serial_number=serial_number, state=state, update_time=update_time, dependency=dependency)\
              .to_file(path=State.get_path(serial_number=serial_number, path=path if path else self.path))

    def get(self, serial_number, path=None):
        ret = StateCode.UNKNOWN
        try:
            ret = State.from_file(path=State.get_path(serial_number=serial_number, path=path if path else self.path)).get()
        finally:
            return ret

    def check(self, serial_number, path=None):
        if isinstance(serial_number, str):
            d = None
            try:
                d = State.from_file(path=State.get_path(serial_number=serial_number, path=path if path else self.path)).to_dict()
            finally:
                return d
        elif isinstance(serial_number, Iterable):
            ret = list()
            for s in serial_number:
                tmp = self.check(s)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def remove(self, serial_number, path=None):
        os.remove(State.get_path(serial_number=serial_number, path=path if path else self.path))


class MongoStateManager(StateManager, MongoDBService):
    def __init__(self, db=None, collection=None,
                 mongodb=None, *args, **kwargs):
        MongoDBService.__init__(self, db=db, collection=collection,
                                mongodb=mongodb, *args, **kwargs)
        StateManager.__init__(self)

    def set(self, id, serial_number, state=None, update_time=None, dependency=None, db=None, collection=None):
        tmp = State(id=id, serial_number=serial_number, state=state, update_time=update_time, dependency=dependency).to_dict()
        _id = tmp.pop('_id')
        self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .update_one({'_id': _id}, {'$set': tmp}, upsert=True)

    def get(self, serial_number, db=None, collection=None):
        tmp = self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .find_one({'_id': serial_number})
        return State.from_dict(tmp).state if tmp else StateCode.UNKNOWN

    def check(self, serial_number, db=None, collection=None):
        if isinstance(serial_number, str):
            return self.mongodb[db if db else self.db][collection if collection else self.collection]\
                .find_one({'_id': serial_number})
        elif isinstance(serial_number, Iterable):
            return list(self.mongodb[db if db else self.db][collection if collection else self.collection]
                        .find({'_id': {'$in': list(serial_number)}}))
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def remove(self, serial_number, db=None, collection=None):
        self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .delete_one({'_id': serial_number})
