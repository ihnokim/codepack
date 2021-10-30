from codepack.abc import MongoDBService
from datetime import datetime
from codepack.state import State


class StateManager(MongoDBService):
    def __init__(self, db=None, collection=None,
                 mongodb=None, online=False, **kwargs):
        super().__init__(db=db, collection=collection,
                         mongodb=mongodb, online=online, **kwargs)

    def update(self, code, state):
        if self.online:
            d = {'code': code.id,
                 'state': state.name, 'update_time': datetime.now()}
            self.mongodb[self.db][self.collection].update_one({'_id': code.serial_number}, {'$set': d}, upsert=True)
        else:
            code.state = state

    def get(self, code):
        if self.online:
            ret = self.mongodb[self.db][self.collection].find_one({'_id': code.serial_number}, projection={'state'})
            state = State[ret['state']] if ret else State.UNKNOWN
        else:
            state = code.state if code.state else State.UNKNOWN
        return state
