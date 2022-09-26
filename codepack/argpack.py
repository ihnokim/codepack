from codepack.arg import Arg
from codepack.utils.config.default import Default
from codepack.storages.storable import Storable
from typing import Optional, TypeVar, Union, Iterator


CodePack = TypeVar('CodePack', bound='codepack.codepack.CodePack')  # noqa: F821
StorageService = TypeVar('StorageService', bound='codepack.plugins.storage_service.StorageService')  # noqa: F821


class ArgPack(Storable):
    def __init__(self, codepack: Optional[CodePack] = None,
                 name: Optional[str] = None,
                 version: Optional[str] = None,
                 owner: Optional[str] = None,
                 timestamp: Optional[float] = None,
                 args: Optional[dict] = None) -> None:
        _name = None
        if codepack:
            _name = codepack.get_name()
        if name:
            _name = name
        Storable.__init__(self, name=_name, version=version, timestamp=timestamp, id_key='_name')
        if args:
            self.args = dict()
            for n, kwargs in args.items():
                self.args[n] = Arg.from_dict(kwargs)
        elif codepack:
            self.args = self.extract(codepack)
        else:
            self.args = dict()
        self.owner = owner

    @staticmethod
    def extract(codepack: CodePack) -> dict:
        ret = dict()
        stack = list()
        for root in codepack.roots:
            stack.append(root)
            while len(stack):
                n = stack.pop(-1)
                if n.get_name() not in ret:
                    ret[n.get_name()] = Arg(n)
                for c in n.children.values():
                    stack.append(c)
        return ret

    def __getitem__(self, item: str) -> Arg:
        return self.args[item]

    def __setitem__(self, key: str, value: Union[Arg, dict]) -> None:
        self.args[key] = value

    def to_dict(self) -> dict:
        d = self.get_metadata()
        d.pop('_serial_number', None)
        d['args'] = dict()
        for name, arg in self.args.items():
            d['args'][name] = arg.to_dict()
        d['owner'] = self.owner
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'ArgPack':
        return cls(name=d.get('_name', None), timestamp=d.get('_timestamp', None),
                   args=d.get('args', None), owner=d.get('owner', None))

    def save(self, update: bool = False, storage_service: Optional[StorageService] = None) -> None:
        if storage_service is None:
            storage_service = Default.get_service('argpack', 'storage_service')
        storage_service.save(item=self, update=update)

    @classmethod
    def load(cls, name: Union[str, list], storage_service: Optional[StorageService] = None)\
            -> Optional[Union['ArgPack', list]]:
        if storage_service is None:
            storage_service = Default.get_service('argpack', 'storage_service')
        return storage_service.load(name=name)

    @classmethod
    def remove(cls, name: Union[str, list], storage_service: Optional[StorageService] = None) -> None:
        if storage_service is None:
            storage_service = Default.get_service('argpack', 'storage_service')
        storage_service.remove(name=name)

    def __getattr__(self, item: str) -> Arg:
        return getattr(self.args, item)

    def __iter__(self) -> Iterator[str]:
        return self.args.__iter__()

    def __str__(self) -> str:
        ret = '%s(name: %s, args: {' % (self.__class__.__name__, self.get_name())
        for i, (name, arg) in enumerate(self.args.items()):
            if i:
                ret += ', '
            ret += '%s%s' % (name, arg.__str__().replace('Arg(', '('))
        ret += '}'
        if self.owner:
            ret += ', owner: %s' % self.owner
        ret += ')'
        return ret

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover
