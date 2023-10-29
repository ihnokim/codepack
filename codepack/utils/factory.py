from codepack.utils.type_manager import TypeManager
from typing import Dict, Optional, Type
from codepack.utils.configurable import Configurable


class Factory:
    def __init__(self, type_manager: Type[TypeManager]):
        self.type_manager = type_manager

    def get_instance(self, type: str, config: Optional[Dict[str, str]] = None) -> Configurable:
        _class = self.type_manager.get(name=type)
        return _class.get_instance(config=config)
