import abc


class Interface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, config, **kwargs):
        """Initialize an instance from config."""
