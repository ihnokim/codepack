import abc
from codepack.storages.memory_message_sender import MemoryMessageSender
from codepack.storages.memory_message_receiver import MemoryMessageReceiver
from typing import Union


class Employee(metaclass=abc.ABCMeta):
    def __init__(self, messenger: Union[MemoryMessageSender, MemoryMessageReceiver]) -> None:
        self.messenger = messenger

    def close(self):
        self.messenger.close()
