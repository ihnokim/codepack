from enum import Enum


class State(Enum):
    UNKNOWN = 0
    NEW = 1
    READY = 2
    WAITING = 3
    RUNNING = 4
    TERMINATED = 5
    ERROR = 6

    def __str__(self):
        return self.name  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        elif isinstance(other, int):
            return self.name == State(other).name
        else:
            return False  # pragma: no cover

    @classmethod
    def get(cls, state):
        if isinstance(state, cls):
            return state
        elif isinstance(state, str):
            return cls[state]
        elif isinstance(state, int):
            return cls(state)
        else:
            return cls.UNKNOWN  # pragma: no cover
