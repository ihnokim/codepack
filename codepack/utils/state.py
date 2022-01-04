from codepack.abc import Storable
from datetime import datetime
from codepack.utils.dependency import Dependency
from codepack.utils.state_code import StateCode


class State(Storable):
    def __init__(self, id, serial_number, state, update_time=None, args=None, kwargs=None, dependency=None):
        super().__init__(id=id)
        self.serial_number = serial_number
        self.state = self.get_state_code(state)
        self.update_time = update_time if update_time else datetime.now().timestamp()
        self.args = None
        self.kwargs = None
        self.dependency = dict()
        self.set_args(args=args, kwargs=kwargs)
        self.set_dependency(dependency=dependency)

    def to_dict(self):
        return {'id': self.id, '_id': self.serial_number,
                'state': self.state.name, 'update_time': self.update_time,
                'args': self.args, 'kwargs': self.kwargs, 'dependency': self.dependency}

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['id'], serial_number=d['_id'], state=StateCode[d['state']], update_time=d.get('update_time', None),
                   args=d.get('args', None), kwargs=d.get('kwargs', None), dependency=d.get('dependency', None))

    def set(self, state, update_time=None, args=None, kwargs=None, dependency=None):
        if update_time is None:
            update_time = datetime.now()
        self.state = self.get_state_code(state)
        self.update_time = update_time
        self.set_args(args=args, kwargs=kwargs)
        self.set_dependency(dependency=dependency)

    def set_args(self, args=None, kwargs=None):
        self.args = list(args) if args else list()
        self.kwargs = kwargs if kwargs else dict()

    def set_dependency(self, dependency=None):
        if dependency:
            for k, v in dependency.items():
                if isinstance(v, Dependency):
                    self.dependency[k] = v.to_dict()
                elif isinstance(v, dict):
                    self.dependency[k] = v
                else:
                    raise TypeError(type(v))  # pragma: no cover

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

    def __str__(self):
        return self.state.__str__()  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover
