import abc
from codepack.service.service import Service


class StorageService(Service, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save(self, item):
        """save item"""

    @abc.abstractmethod
    def load(self, id):
        """load item with given id"""

    @abc.abstractmethod
    def remove(self, id):
        """remove item with given id"""

    @abc.abstractmethod
    def check(self, id):
        """check if item with given id exists"""
