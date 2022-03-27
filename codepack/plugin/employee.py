from codepack.storage.messenger import Messenger
import abc


class Employee(metaclass=abc.ABCMeta):
    def __init__(self, messenger: Messenger):
        self.messenger = messenger

    def close(self):
        self.messenger.close()
