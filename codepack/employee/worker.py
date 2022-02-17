import requests
from codepack import Code
from codepack.snapshot import CodeSnapshot
from codepack.employee.supervisor import Supervisor
from codepack.storage import KafkaStorage
from codepack.config import Default
from codepack.docker import DockerManager
import os


class Worker(KafkaStorage):
    def __init__(self, consumer=None, interval=1, callback=None, supervisor=None, docker_manager=None, consumer_config=None):
        KafkaStorage.__init__(self, consumer=consumer, consumer_config=consumer_config)
        self.interval = interval
        self.supervisor = supervisor
        self.docker_manager = None
        self.callback = callback
        if self.supervisor and not self.callback:
            if isinstance(self.supervisor, str) or isinstance(self.supervisor, Supervisor):
                self.callback = self.inform_supervisor_of_termination
            else:
                self.close()
                raise TypeError(type(self.supervisor))
        self.register(callback=self.callback)
        self.init_docker_manager(docker_manager=docker_manager)

    def init_docker_manager(self, docker_manager):
        if docker_manager is None:
            self.docker_manager = Default.get_docker_manager()
        elif isinstance(docker_manager, DockerManager):
            self.docker_manager = docker_manager
        else:
            raise TypeError(type(docker_manager))

    def register(self, callback):
        self.callback = callback

    def start(self):
        self.consumer.consume(self.work, timeout_ms=int(float(self.interval) * 1000))

    def stop(self):
        pass

    def work(self, buffer):
        for tp, msgs in buffer.items():
            for msg in msgs:
                snapshot = CodeSnapshot.from_dict(msg.value)
                self.run_snapshot(snapshot=snapshot)

    def run_snapshot(self, snapshot: CodeSnapshot):
        code = Code.from_snapshot(snapshot)
        try:
            if code.image:
                filepath = '%s.json' % code.serial_number
                full_filepath = os.path.join(self.docker_manager.path, filepath)
                snapshot.to_file(full_filepath)
                ret = self.docker_manager.run(image=code.image, command=['python', 'run_code.py', filepath])
                print(ret.decode('utf-8'))
                self.docker_manager.remove_file_if_exists(path=full_filepath)
                if self.callback:
                    self.callback({'state': code.get_state(), 'serial_number': code.serial_number})
            else:
                code.register(callback=self.callback)
                code(*snapshot.args, **snapshot.kwargs)
        except Exception as e:
            print(e)  # log.error(e)
            if code is not None:
                code.update_state('ERROR')

    def inform_supervisor_of_termination(self, x):
        if x['state'] == 'TERMINATED':
            if isinstance(self.supervisor, str):
                requests.get(self.supervisor + '/organize/%s' % x['serial_number'])
            elif isinstance(self.supervisor, Supervisor):
                self.supervisor.organize(x['serial_number'])
