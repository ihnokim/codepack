import abc
from typing import Union, Any


class Manager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self, command: Union[str, list], **kwargs: Any) -> Any:
        """run with given command"""
