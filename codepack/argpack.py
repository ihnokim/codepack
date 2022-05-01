from codepack.arg import Arg
from codepack.utils.config.default import Default
from codepack.storages.storable import Storable
from typing import Optional, TypeVar, Union, Iterator


CodePack = TypeVar('CodePack', bound='codepack.codepack.CodePack')
StorageService = TypeVar('StorageService', bound='codepack.plugins.storage_service.StorageService')


class ArgPack(Storable):
    def __init__(self, codepack: Optional[CodePack] = None, id: Optional[str] = None,
                 args: Optional[dict] = None) -> None:
        _id = None
        if codepack:
            _id = codepack.id
        if id:
            _id = id
        Storable.__init__(self, id=_id)
        if args:
            self.args = dict()
            for id, kwargs in args.items():
                self.args[id] = Arg.from_dict(kwargs)
        elif codepack:
            self.args = self.extract(codepack)
        else:
            self.args = dict()

    @staticmethod
    def extract(codepack: CodePack) -> dict:
        ret = dict()
        stack = list()
        for root in codepack.roots:
            stack.append(root)
            while len(stack):
                n = stack.pop(-1)
                if n.id not in ret:
                    ret[n.id] = Arg(n)
                for c in n.children.values():
                    stack.append(c)
        return ret

    def __getitem__(self, item: str) -> Arg:
        return self.args[item]

    def __setitem__(self, key: str, value: Union[Arg, dict]) -> None:
        self.args[key] = value

    def to_dict(self) -> dict:
        ret = dict()
        for id, arg in self.args.items():
            ret[id] = arg.to_dict()
        ret['_id'] = self.id
        return ret

    @classmethod
    def from_dict(cls, d: dict) -> 'ArgPack':
        args = dict()
        id = None
        for k, v in d.items():
            if k == '_id':
                id = v
            else:
                args[k] = v
        return cls(id=id, args=args)

    def save(self, update: bool = False, storage_service: Optional[StorageService] = None) -> None:
        if storage_service is None:
            storage_service = Default.get_service('argpack', 'storage_service')
        storage_service.save(item=self, update=update)

    @classmethod
    def load(cls, id: Union[str, list], storage_service: Optional[StorageService] = None)\
            -> Optional[Union['ArgPack', list]]:
        if storage_service is None:
            storage_service = Default.get_service('argpack', 'storage_service')
        return storage_service.load(id)

    @classmethod
    def remove(cls, id: Union[str, list], storage_service: Optional[StorageService] = None) -> None:
        if storage_service is None:
            storage_service = Default.get_service('argpack', 'storage_service')
        storage_service.remove(id=id)

    def __getattr__(self, item: str) -> Arg:
        return getattr(self.args, item)

    def __iter__(self) -> Iterator[str]:
        return self.args.__iter__()

    def __str__(self) -> str:
        ret = '%s(id: %s, args: {' % (self.__class__.__name__, self.id)
        for i, (id, arg) in enumerate(self.args.items()):
            if i:
                ret += ', '
            ret += '%s%s' % (id, arg.__str__().replace('Arg(', '('))
        ret += '})'
        return ret

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover
