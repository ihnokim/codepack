from codepack.code import Code
from codepack.snapshot.code_snapshot import CodeSnapshot
from codepack.employee.supervisor import Supervisor
from codepack.storage.kafka_storage import KafkaStorage
from codepack.storage.file_storage import FileStorage
from codepack.config.default import Default
from codepack.employee.docker_manager import DockerManager
from codepack.employee.interpreter_manager import InterpreterManager
from codepack.service.callback_service import CallbackService
from codepack.callback.functions import inform_supervisor_of_termination
from functools import partial
import logging
import os
import sys


class Worker(KafkaStorage):
    def __init__(self, consumer=None, interval=1, path=None, script='run_snapshot.py', callback=None,
                 supervisor=None, docker_manager=None, interpreter_manager=None,
                 callback_service=None, consumer_config=None, logger=None):
        KafkaStorage.__init__(self, consumer=consumer, consumer_config=consumer_config)
        self.interval = interval
        self.supervisor = supervisor
        self.docker_manager = None
        self.interpreter_manager = None
        self.callback_service = None
        self.path = path
        self.script = script
        self.callback = callback
        if logger:
            if isinstance(logger, logging.Logger):
                self.logger = logger
            elif isinstance(logger, str):
                self.logger = Default.get_logger(logger)
            else:
                raise TypeError(logger)
        else:
            self.logger = None
        if self.logger:
            sys.stdout.write = partial(self.log, self.logger.info)
        print('initializing worker...')
        if self.supervisor and not self.callback:
            if isinstance(self.supervisor, str) or isinstance(self.supervisor, Supervisor):
                self.callback = partial(inform_supervisor_of_termination, supervisor=self.supervisor)
            else:
                self.close()
                raise TypeError(type(self.supervisor))
        self.init_docker_manager(docker_manager=docker_manager)
        self.init_interpreter_manager(interpreter_manager=interpreter_manager)
        self.init_callback_service(callback_service=callback_service)

    @staticmethod
    def log(function, message):
        _message = message.strip()
        if _message:
            function(_message)

    def init_docker_manager(self, docker_manager):
        if docker_manager is None:
            self.docker_manager = Default.get_docker_manager()
        elif isinstance(docker_manager, DockerManager):
            self.docker_manager = docker_manager
        else:
            raise TypeError(type(docker_manager))

    def init_interpreter_manager(self, interpreter_manager):
        if interpreter_manager is None:
            self.interpreter_manager = Default.get_interpreter_manager()
        elif isinstance(interpreter_manager, InterpreterManager):
            self.interpreter_manager = interpreter_manager
        else:
            raise TypeError(type(interpreter_manager))

    def init_callback_service(self, callback_service):
        if callback_service is None:
            self.callback_service = Default.get_service('callback', 'callback_service')
        elif isinstance(callback_service, CallbackService):
            self.callback_service = callback_service
        else:
            raise TypeError(type(callback_service))

    def start(self):
        print('starting worker...')
        self.consumer.consume(self.work, timeout_ms=int(float(self.interval) * 1000))

    def stop(self):
        print('stopping worker...')
        self.close()

    def work(self, buffer):
        for tp, msgs in buffer.items():
            for msg in msgs:
                snapshot = CodeSnapshot.from_dict(msg.value)
                self.run_snapshot(snapshot=snapshot)

    def run_snapshot(self, snapshot: CodeSnapshot):
        snapshot_path = None
        cb_id = None
        code = Code.from_snapshot(snapshot)
        code.register_callback(callback=self.callback)
        try:
            if code.image or code.env:
                state = code.check_dependency()
                cb_id = self.callback_service.push(self.callback)
                if state == 'READY':
                    filepath = '%s.json' % code.serial_number
                    snapshot_path = os.path.join(self.path, filepath)
                    script_path = os.path.join(self.path, self.script)
                    snapshot.to_file(snapshot_path)
                    if code.env:
                        _command = ['python', script_path, snapshot_path, '-c', cb_id]
                        if isinstance(self.callback_service.storage, FileStorage):
                            _command.append('-p')
                            _command.append(self.callback_service.storage.path)
                        if self.logger:
                            _command.append('-l')
                            _command.append(self.logger.name)
                        self.interpreter_manager.run(env=code.env, command=_command)
                    else:  # if code.image:
                        _command = ['python', self.script, filepath, '-c', cb_id]
                        if isinstance(self.callback_service.storage, FileStorage):
                            _command.append('-p')
                            _command.append('.')
                        if self.logger:
                            _command.append('-l')
                            _command.append(self.logger.name)
                        ret = self.docker_manager.run(image=code.image, command=_command, path=self.path)
                        print(ret.decode('utf-8').strip())
                else:
                    code.update_state(state, args=snapshot.args, kwargs=snapshot.kwargs)
            else:
                code(*snapshot.args, **snapshot.kwargs)
        except Exception as e:
            if self.logger:
                self.logger.error(e)
            else:
                print(e)
            if code is not None:
                code.update_state('ERROR', args=snapshot.args, kwargs=snapshot.kwargs, message=str(e))
        finally:
            if snapshot_path:
                self.docker_manager.remove_file_if_exists(path=snapshot_path)
            if cb_id and self.callback_service.exist(name=cb_id):
                self.callback_service.remove(name=cb_id)
