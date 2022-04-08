from codepack.storages.messenger import Messenger
import abc


class Employee(metaclass=abc.ABCMeta):
    def __init__(self, messenger: Messenger) -> None:
        self.messenger = messenger

    def close(self) -> None:
        self.messenger.close()
