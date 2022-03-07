from codepack import Code
from codepack.snapshot import CodeSnapshot
from codepack.employee.supervisor import Supervisor
from codepack.storage import KafkaStorage
from codepack.config import Default
from codepack.docker import DockerManager
from codepack.callback.functions import inform_supervisor_of_termination
from functools import partial
import os


class Worker(KafkaStorage):
    def __init__(self, consumer=None, interval=1, script='run_snapshot.py', callback=None,
                 supervisor=None, docker_manager=None, consumer_config=None):
        KafkaStorage.__init__(self, consumer=consumer, consumer_config=consumer_config)
        self.interval = interval
        self.supervisor = supervisor
        self.docker_manager = None
        self.script = script
        self.callback = callback
        if self.supervisor and not self.callback:
            if isinstance(self.supervisor, str) or isinstance(self.supervisor, Supervisor):
                self.callback = partial(inform_supervisor_of_termination, supervisor=self.supervisor)
            else:
                self.close()
                raise TypeError(type(self.supervisor))
        self.init_docker_manager(docker_manager=docker_manager)

    def init_docker_manager(self, docker_manager):
        if docker_manager is None:
            self.docker_manager = Default.get_docker_manager()
        elif isinstance(docker_manager, DockerManager):
            self.docker_manager = docker_manager
        else:
            raise TypeError(type(docker_manager))

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
        full_filepath = None
        code = Code.from_snapshot(snapshot)
        code.register_callback(callback=self.callback)
        try:
            if code.image:
                state = code.check_dependency()
                code.update_state(state, args=snapshot.args, kwargs=snapshot.kwargs)
                if state == 'READY':
                    filepath = '%s.json' % code.serial_number
                    full_filepath = os.path.join(self.docker_manager.path, filepath)
                    snapshot.to_file(full_filepath)
                    ret = self.docker_manager.run(image=code.image, command=['python', self.script, filepath])
                    print(ret.decode('utf-8').strip())
                code.run_callback()
            else:
                code(*snapshot.args, **snapshot.kwargs)
        except Exception as e:
            print(e)
            if code is not None:
                code.update_state('ERROR', args=snapshot.args, kwargs=snapshot.kwargs, message=str(e))
        finally:
            if full_filepath:
                self.docker_manager.remove_file_if_exists(path=full_filepath)
