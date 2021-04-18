from enum import Enum


class Status(Enum):
    NEW = 1
    READY = 2
    WAITING = 3
    RUNNING = 4
    TERMINATED = 5
