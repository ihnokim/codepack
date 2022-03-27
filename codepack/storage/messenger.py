import abc


class Messenger(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @abc.abstractmethod
    def init(self, *args, **kwargs):
        """initialize storage"""

    @abc.abstractmethod
    def close(self):
        """close storage"""

    @abc.abstractmethod
    def send(self, item: object, *args, **kwargs):
        """send item"""

    @abc.abstractmethod
    def receive(self, *args, **kwargs):
        """receive item"""
