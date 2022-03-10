import abc
from typing import Union


class Manager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self, command: Union[str, list], **kwargs):
        """run with given command"""
