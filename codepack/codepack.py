from typing import Any, Optional, Dict, Set, List, Iterator, Union
from queue import Queue
import namesgenerator
from codepack.utils.observer import Observer
from codepack.code import Code
from codepack.argpack import ArgPack
from codepack.item import Item
from codepack.utils.double_key_map import DoubleKeyMap


class CodePack(Item, Observer):
    def __init__(self,
                 codes: List[Code],
                 name: Optional[str] = None,
                 version: Optional[str] = None,
                 owner: Optional[str] = None,
                 description: Optional[str] = None,
                 subscription: Optional[Union[str, Code]] = None):
        super().__init__(name=name if name else namesgenerator.get_random_name(sep='-'),
                         version=version,
                         owner=owner,
                         description=description)
        self.codes: Dict[str, Code] = dict()
        self.links: DoubleKeyMap = DoubleKeyMap()
        self.subscription: Optional[str]
        if subscription and isinstance(subscription, Code):
            self.subscription = subscription.get_id()
        else:
            self.subscription = subscription
        if len(codes) == 0:
            raise ValueError('At least one code is required')
        for code in codes:
            self.add_code(code=code, update=False)
        self.update()

    def get_id(self) -> str:
        return self.get_fullname()

    def _get_roots(self) -> Set[Code]:
        roots: Set[Code] = set()
        for code in self.traverse_codes():
            if len(code.upstream) == 0:
                roots.add(code)
        return roots

    def traverse_codes(self) -> Set[Code]:
        basket: Set[Code] = set()
        q = Queue()
        for code in self.codes.values():
            q.put(code)
        while not q.empty():
            code: Code = q.get()
            basket.add(code)
            for adj_code in (code.upstream | code.downstream):
                if adj_code not in basket:
                    q.put(adj_code)
        return basket

    def _check_if_code_is_included_in_other_codepack(self, code: Code) -> None:
        if len(code.observable.observers) != 0 and code.observable.get_observer(id=self.get_id()) is None:
            raise ValueError(f"'{code.get_id()}' is included in other codepack")

    def collect_codes(self) -> None:
        new_codes: Set[Code] = set()
        for code in self.traverse_codes():
            if code.get_id() not in self.codes:
                self._check_if_code_is_included_in_other_codepack(code=code)
                new_codes.add(code)
        for code in new_codes:
            self.add_code(code=code, update=False)

    def add_code(self, code: Code, update: bool = True) -> None:
        self._check_if_code_is_included_in_other_codepack(code=code)
        if len(code.observable.observers) == 0:
            code.observable.register_observer(id=self.get_id(), observer=self)
        self.codes[code.get_id()] = code
        if update:
            self.update(observable=code.observable)

    def remove_code(self, code: Code, update: bool = True) -> None:
        if code.get_id() not in self.codes.keys():
            raise KeyError(f"'{code.get_id()}' not found")
        self._check_if_code_is_included_in_other_codepack(code=code)
        self.codes.pop(code.get_id())
        code.observable.unregister_observer(id=self.get_id())
        if update:
            self.update(observable=code.observable)

    def __add__(self, code: Code) -> 'CodePack':
        self.add_code(code=code)
        return self

    def __sub__(self, code: Code) -> 'CodePack':
        self.remove_code(code=code)
        return self

    def update(self, observable: Optional[Code] = None, **kwargs: Any) -> None:
        self.collect_codes()
        backup = self.links.to_dict()
        try:
            self.links.remove_all()
            for code in self.codes.values():
                for child_code in code.downstream:
                    src = code.get_id()
                    dst = child_code.get_id()
                    if not self.links.contains(key1=src, key2=dst):
                        self.links.put(key1=src, key2=dst)
                for parent_code in code.upstream:
                    src = parent_code.get_id()
                    dst = code.get_id()
                    if not self.links.contains(key1=src, key2=dst):
                        self.links.put(key1=src, key2=dst)
                for param, src in code.dependencies.items():
                    self.links.put(key1=src.get_id(), key2=code.get_id(), value=param)
        except Exception as e:
            self.links = DoubleKeyMap(map=backup)
            raise e

    def __call__(self, argpack: Optional[ArgPack] = None) -> Any:
        results = dict()
        executed: Set[str] = set()
        q = Queue()
        for code in self._get_roots():
            q.put(code)
        while not q.empty():
            code = q.get()
            if code.get_id() in executed:
                continue
            dependencies = {param: results[src.get_id()] for param, src in code.dependencies.items()}
            if argpack:
                arg = argpack.get_arg_by_code_id(code_id=code.get_id())
                if arg:
                    results[code.get_id()] = code(*arg.args, **arg.kwargs, **dependencies)
                else:
                    results[code.get_id()] = code(**dependencies)
            else:
                results[code.get_id()] = code(**dependencies)
            executed.add(code.get_id())
            for child in code.downstream:
                q.put(child)
        if self.subscription:
            return results[self.subscription]
        else:
            return None

    def __serialize__(self) -> Dict[str, Any]:
        return {'codes': sorted([code.to_dict() for code in self.codes.values()], key=lambda x: x['_id']),
                'links': self.links.to_dict(),
                'name': self.name,
                'version': self.version,
                'owner': self.owner,
                'description': self.description,
                'subscription': self.subscription}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'CodePack':
        links = DoubleKeyMap(map=d['links'])
        codes = {c['_id']: Code.from_dict(c) for c in d['codes']}
        for src, dst, param in links.items():
            code1 = codes[src]
            code2 = codes[dst]
            if param:
                code1 >> code2 | param
            else:
                code1 > code2
        return cls(codes=list(codes.values()),
                   name=d['name'],
                   version=d['version'],
                   owner=d['owner'],
                   description=d['description'],
                   subscription=d['subscription'])

    def __iter__(self) -> Iterator[str]:
        return self.codes.values().__iter__()
