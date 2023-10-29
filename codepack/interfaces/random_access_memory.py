from typing import Any, Dict, Union, OrderedDict
from codepack.interfaces.interface import Interface
from codepack.utils.parser import Parser
from copy import deepcopy
from collections import OrderedDict as odict


class RandomAccessMemory(Interface):
    def __init__(self, keep_order: bool = False) -> None:
        self.keep_order = keep_order
        self._is_closed = False
        self.connect()

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        _config = deepcopy(config)
        if 'keep_order' in _config:
            _config['keep_order'] = Parser.parse_bool(_config['keep_order'])
        return _config

    def connect(self) -> None:
        self._is_closed = False
        if self.keep_order:
            self._memory = odict()
        else:
            self._memory = dict()
        return self._memory

    def close(self) -> None:
        self._memory.clear()
        self._is_closed = True

    def is_closed(self) -> bool:
        return self._is_closed

    def get_session(self) -> Union[Dict[str, Any], OrderedDict[str, Any]]:
        return self._memory
