from codepack.code import Code
from codepack.codepack import CodePack
from codepack.config.default import Default
from codepack.snapshot.code_snapshot import CodeSnapshot
from codepack.storage.kafka_storage import KafkaStorage


class Supervisor(KafkaStorage):
    def __init__(self, producer=None, topic=None, snapshot_service=None, producer_config=None):
        KafkaStorage.__init__(self, producer=producer, topic=topic, producer_config=producer_config)
        self.snapshot_service =\
            snapshot_service if snapshot_service else Default.get_service('code_snapshot', 'snapshot_service')

    def run_code(self, code, args=None, kwargs=None):
        if isinstance(code, Code):
            code.update_state('READY')
            if kwargs and not isinstance(kwargs, dict):
                _kwargs = kwargs.to_dict()
            else:
                _kwargs = kwargs
            self.producer.produce(self.topic, value=code.to_snapshot(args=args, kwargs=_kwargs).to_dict())
        else:
            raise TypeError(type(code))

    def run_codepack(self, codepack, argpack=None):
        if isinstance(codepack, CodePack):
            codepack.save_snapshot(argpack=argpack)
            codepack.init_code_state(state='READY', argpack=argpack)
            for id, code in codepack.codes.items():
                _kwargs = argpack[id] if id in argpack else None
                self.run_code(code=code, kwargs=_kwargs)
        else:
            raise TypeError(type(codepack))

    def organize(self, serial_number=None):
        for snapshot in self.snapshot_service.search(key='state', value='WAITING'):
            resolved = True
            dependent_serial_numbers = [snapshot['serial_number'] for snapshot in snapshot['dependency']]
            dependencies = self.snapshot_service.load(dependent_serial_numbers, projection={'state'})
            if serial_number and serial_number not in {dependency['serial_number'] for dependency in dependencies}:
                continue
            for dependency in dependencies:
                if dependency['state'] != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                code_snapshot = CodeSnapshot.from_dict(snapshot)
                code = Code.from_snapshot(code_snapshot)
                self.run_code(code=code, args=code_snapshot.args, kwargs=code_snapshot.kwargs)
