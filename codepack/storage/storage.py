import abc


class Storage(metaclass=abc.ABCMeta):
    def __init__(self, obj=None):
        self.obj = obj

    @abc.abstractmethod
    def init(self, *args, **kwargs):
        """initialize storage"""
