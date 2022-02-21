import abc
from codepack.service.service import Service


class DeliveryService(Service, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def send(self, id, serial_number, item=None, timestamp=None):
        """send item"""

    @abc.abstractmethod
    def receive(self, serial_number):
        """receive item"""

    @abc.abstractmethod
    def cancel(self, serial_number):
        """cancel delivery"""

    @abc.abstractmethod
    def check(self, serial_number):
        """check arrival of delivery"""
