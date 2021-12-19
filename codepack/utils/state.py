from codepack.abc import Storable
from enum import Enum
from datetime import datetime


class StateCode(Enum):
    UNKNOWN = 0
    NEW = 1
    READY = 2
    WAITING = 3
    RUNNING = 4
    TERMINATED = 5
    ERROR = 6

    def __str__(self):
        return self.name  # pragma: no cover

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        elif isinstance(other, State):
            return self == other.state
        elif isinstance(other, dict):
            return self == StateCode[other['state']]
        elif isinstance(other, int):
            return self.name == StateCode(other).name
        else:
            return False  # pragma: no cover


class State(Storable):
    def __init__(self, id, serial_number, state, update_time=None, dependency=None):
        super().__init__(id=id)
        self.serial_number = serial_number
        self.state = self.get_state_code(state)
        self.update_time = update_time if update_time else datetime.now().timestamp()
        self.dependency = dict()
        self.set_dependency(dependency=dependency)

    def to_dict(self):
        return {'id': self.id, '_id': self.serial_number,
                'state': self.state.name, 'update_time': self.update_time, 'dependency': self.dependency}

    @classmethod
    def from_dict(cls, d, *args, **kwargs):
        return cls(id=d['id'], serial_number=d['_id'], state=StateCode[d['state']],
                   update_time=d.get('update_time', None), dependency=d.get('dependency', None))

    def set(self, state, update_time=None, dependency=None):
        if update_time is None:
            update_time = datetime.now()
        self.state = self.get_state_code(state)
        self.update_time = update_time
        self.set_dependency(dependency=dependency)

    def set_dependency(self, dependency=None):
        if dependency:
            for k, v in dependency.items():
                self.dependency[k] = v.to_dict()

    def get(self):
        return self.state

    @staticmethod
    def get_state_code(state):
        if isinstance(state, StateCode):
            return state
        elif isinstance(state, str):
            return StateCode[state]
        elif isinstance(state, int):
            return StateCode(state)
        else:
            raise TypeError(type(state))  # pragma: no cover

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.state == other.state
        else:
            return self.state == other