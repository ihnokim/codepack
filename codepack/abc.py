import abc


class AbstractCode(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        """Initialize an instance."""
