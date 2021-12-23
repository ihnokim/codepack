import abc
from codepack.service import DefaultServicePack


class Engine(metaclass=abc.ABCMeta):
    def __init__(self, callback, interval=1, config_path=None, state_manager=None):
        """initialize an instance"""
        self.callback = callback
        self.interval = interval
        self.state_manager = state_manager if state_manager else DefaultServicePack.get_default_state_manager(config_path=config_path)

    @abc.abstractmethod
    def start(self):
        """Start loop"""

    @abc.abstractmethod
    def stop(self):
        """Stop loop"""

    def work(self):
        for x in self.state_manager.search('WAITING'):
            resolved = True
            for s in self.state_manager.get(x['dependency'].keys()):
                if s != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                self.callback(x)
