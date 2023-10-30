from typing import Dict, Any, Optional, List, Iterator
from copy import deepcopy
import namesgenerator
from codepack.arg import Arg
from codepack.item import Item


class ArgPack(Item):
    def __init__(self,
                 args: List[Arg],
                 name: Optional[str] = None,
                 version: Optional[str] = None,
                 owner: Optional[str] = None,
                 description: Optional[str] = None,
                 code_map: Optional[Dict[str, str]] = None,
                 ) -> None:
        self.args: Dict[str, Arg] = dict()
        self.code_map: Dict[str, str] = dict()
        if len(args) == 0:
            raise ValueError('At least one arg is required')
        for arg in args:
            self.args[arg.get_id()] = arg
        if code_map:
            for c, a in code_map.items():
                self.map_code(code_id=c, arg_id=a)
        super().__init__(name=name if name else namesgenerator.get_random_name(sep='-'),
                         version=version,
                         owner=owner,
                         description=description)

    def get_arg_by_code_id(self, code_id: str) -> Optional[Arg]:
        if code_id in self.code_map.keys():
            arg_id = self.code_map[code_id]
            return self.args[arg_id]
        else:
            return None

    def map_code(self, code_id: str, arg_id: str) -> None:
        if arg_id not in self.args.keys():
            raise KeyError(f"'{arg_id}'")
        self.code_map[code_id] = arg_id

    def unmap_code(self, code_id: str) -> None:
        self.code_map.pop(code_id, None)

    def get_code_map(self) -> Dict[str, str]:
        return deepcopy(self.code_map)

    def get_id(self) -> str:
        return self.get_fullname()

    def __serialize__(self) -> Dict[str, Any]:
        return {'name': self.name,
                'version': self.version,
                'owner': self.owner,
                'description': self.description,
                'args': [arg.to_dict() for arg in self.args.values()],
                'code_map': deepcopy(self.code_map)}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> Item:
        return cls(name=d['name'],
                   version=d['version'],
                   owner=d['owner'],
                   description=d['description'],
                   args=[Arg.from_dict(arg) for arg in d['args']],
                   code_map=d['code_map'])

    def __getitem__(self, param: str) -> Optional[Arg]:
        return self.args[param] if param in self.args else None

    def __add__(self, arg: Arg) -> 'ArgPack':
        self.args[arg.get_id()] = arg
        return self

    def __sub__(self, arg: Arg) -> 'ArgPack':
        if len(self.args) == 1 and arg.get_id() in self.args.keys():
            raise ValueError('At least one arg is required')
        self.args.pop(arg.get_id(), None)
        return self

    def __iter__(self) -> Iterator[str]:
        return self.args.values().__iter__()
