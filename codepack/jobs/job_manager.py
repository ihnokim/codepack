import abc
from typing import Any, Dict, Union
from copy import deepcopy
from codepack.messengers.sendable import Sendable
from codepack.messengers.receivable import Receivable
from codepack.jobs.job import Job
from codepack.utils.configurable import Configurable
from codepack.utils.config import Config
from codepack.utils.factory import Factory
from codepack.messengers.sender_type import SenderType
from codepack.messengers.receiver_type import ReceiverType
from codepack.utils.parser import Parser


class JobManager(Configurable, metaclass=abc.ABCMeta):
    def __init__(self,
                 topic: str,
                 sender: Sendable,
                 receiver: Receivable,
                 background: bool = True) -> None:
        self.topic = topic
        self.sender = sender
        self.receiver = receiver
        self.background = background

    @abc.abstractmethod
    def handle(self, job: Job) -> None:
        pass  # pragma: no cover

    def start(self, interval: float = 1.0) -> None:
        self.receiver.poll(callback=self._process_message,
                           interval=interval,
                           background=self.background)

    def stop(self) -> None:
        self.receiver.stop()

    def notify(self, job: Job) -> None:
        self.sender.send(topic=self.topic, message=job.to_json())

    def _process_message(self, message: Union[str, bytes]) -> None:
        self.handle(job=Job.from_json(message))

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        config_obj = Config()
        _config = deepcopy(config)
        if 'sender' in _config:
            factory = Factory(type_manager=SenderType)
            sender_section = _config['sender']
            sender_config = config_obj.get_config(section=sender_section)
            sender_type = sender_config.get('type')
            sender_args_section = sender_config.get('config')
            sender_args = config_obj.get_config(section=sender_args_section)
            _config['sender'] = factory.get_instance(type=sender_type,
                                                     config=sender_args)
        if 'receiver' in _config:
            factory = Factory(type_manager=ReceiverType)
            receiver_section = _config['receiver']
            receiver_config = config_obj.get_config(section=receiver_section)
            receiver_type = receiver_config.get('type')
            receiver_args_section = receiver_config.get('config')
            receiver_args = config_obj.get_config(section=receiver_args_section)
            _config['receiver'] = factory.get_instance(type=receiver_type,
                                                       config=receiver_args)
        if 'background' in _config:
            _config['background'] = Parser.parse_bool(_config['background'])
        return _config
