import abc
from codepack.service.service import Service


class DeliveryService(Service, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def send(self, sender, invoice_number, item=None, send_time=None):
        """send item"""

    @abc.abstractmethod
    def receive(self, invoice_number):
        """receive item"""

    @abc.abstractmethod
    def cancel(self, invoice_number):
        """cancel delivery"""


class StateManager(Service, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set(self, id, serial_number, state, update_time=None, args=None, kwargs=None, dependency=None):
        """set state"""

    @abc.abstractmethod
    def get(self, serial_number):
        """get state"""

    @abc.abstractmethod
    def remove(self, serial_number):
        """remove state"""

    @abc.abstractmethod
    def search(self, state):
        """search by state"""


class StorageService(Service, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save(self, obj):
        """save object"""

    @abc.abstractmethod
    def load(self, id):
        """load object with given id"""

    @abc.abstractmethod
    def remove(self, id):
        """remove object with given id"""
