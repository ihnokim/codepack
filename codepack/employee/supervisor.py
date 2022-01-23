from codepack import Code
from codepack.service import DefaultService
from codepack.snapshot import CodeSnapshot
from codepack.interface import KafkaProducer
from codepack.utils.config import get_default_service_config


class Supervisor:
    def __init__(self, producer=None, topic=None, config_path=None, snapshot_service=None):
        self.producer = producer
        self.topic = topic
        if self.producer is None:
            config = get_default_service_config('supervisor', config_path=config_path)
            self.producer = KafkaProducer(config['kafka'])
            if self.topic is None:
                self.topic = config['topic']
        self.snapshot_service = snapshot_service if snapshot_service else DefaultService.get_default_code_snapshot_service(config_path=config_path)

    def order(self, code, args=None, kwargs=None):
        if isinstance(code, Code):
            self.producer.produce(self.topic, value=code.to_snapshot(args=args, kwargs=kwargs).to_dict())
        elif isinstance(code, dict):
            self.producer.produce(self.topic, value=code)
        elif isinstance(code, CodeSnapshot):
            self.producer.produce(self.topic, value=code.to_dict())
        else:
            raise TypeError(type(code))

    def organize(self):
        for snapshot in self.snapshot_service.search(key='state', value='WAITING'):
            resolved = True
            serial_numbers = [snapshot['serial_number'] for snapshot in snapshot['dependency']]
            for dependency in self.snapshot_service.load(serial_numbers, projection={'state'}):
                if dependency['state'] != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                self.order(snapshot)
