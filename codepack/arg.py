from codepack.item import Item
from typing import Optional, Any, Dict, List
from copy import deepcopy


class Arg(Item):
    def __init__(self,
                 name: Optional[str] = None,
                 version: Optional[str] = None,
                 owner: Optional[str] = None,
                 description: Optional[str] = None,
                 args: Optional[List[Any]] = None,
                 kwargs: Optional[Dict[str, Any]] = None) -> None:
        self.args = list()
        self.kwargs = dict()
        if args:
            for arg in args:
                self.args.append(arg)
        if kwargs:
            for param, value in kwargs.items():
                self.kwargs[param] = value
        super().__init__(name=name,
                         version=version,
                         owner=owner,
                         description=description)

    def get_id(self) -> str:
        return self.get_fullname()

    def __serialize__(self) -> Dict[str, Any]:
        return {'name': self.name,
                'version': self.version,
                'owner': self.owner,
                'description': self.description,
                'args': deepcopy(self.args),
                'kwargs': deepcopy(self.kwargs)}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> Item:
        return cls(name=d['name'],
                   version=d['version'],
                   owner=d['owner'],
                   description=d['description'],
                   args=d['args'],
                   kwargs=d['kwargs'])

    def __getitem__(self, param: str) -> Any:
        return self.kwargs[param]

    def __setitem__(self, param: str, value: Any) -> None:
        self.kwargs[param] = value

    def __call__(self, *args: Any, **kwargs: Any) -> 'Arg':
        self.args = list(args)
        self.kwargs = deepcopy(kwargs)
        return self
