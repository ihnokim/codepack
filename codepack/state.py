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
        return self.name
