from codepack.config.default import Default
import abc


class Engine(metaclass=abc.ABCMeta):
    def __init__(self, callback, interval=1, config_path=None, snapshot_service=None):
        """initialize instance"""
        self.callback = callback
        self.interval = interval
        self.snapshot_service =\
            snapshot_service if snapshot_service else Default.get_service('code_snapshot', 'snapshot_service',
                                                                          config_path=config_path)

    @abc.abstractmethod
    def start(self):
        """Start loop"""

    @abc.abstractmethod
    def stop(self):
        """Stop loop"""

    def work(self):
        for x in self.snapshot_service.search(key='state', value='WAITING'):
            resolved = True
            for s in self.snapshot_service.load([x['serial_number'] for x in x['dependency']], projection={'state'}):
                if s['state'] != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                self.callback(x)
