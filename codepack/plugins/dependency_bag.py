from codepack.storages.memory_storage import MemoryStorage
from codepack.plugins.dependency import Dependency
from collections.abc import Iterable
from typing import Union, Optional, Type, TypeVar, KeysView, ValuesView, ItemsView, Iterator


Code = TypeVar('Code', bound='codepack.code.Code')
CodeSnapshot = TypeVar('CodeSnapshot', bound='codepack.plugins.snapshots.code_snapshot.CodeSnapshot')


class DependencyBag(MemoryStorage, Iterable):
    def __init__(self, code: Code, item_type: Type[Dependency] = Dependency) -> None:
        MemoryStorage.__init__(self, item_type=item_type)
        self.code = code

    def add(self, dependency: Union[Dependency, dict, Iterable]) -> None:
        if isinstance(dependency, Dependency):
            self.code.assert_param(dependency.param)
            self.memory[dependency.serial_number] = dependency
        elif isinstance(dependency, dict):
            self.code.assert_param(dependency['param'])
            d = Dependency.from_dict(d=dependency)
            d.bind(self.code)
            self.memory[dependency['serial_number']] = d
        elif isinstance(dependency, Iterable):
            for d in dependency:
                self.add(d)
        else:
            raise TypeError(type(dependency))  # pragma: no cover

    def __getitem__(self, item: str) -> Dependency:
        return self.memory[item]

    def remove(self, serial_number: str) -> None:
        self.memory.pop(serial_number, None)

    def get_params(self) -> dict:
        ret = dict()
        for dependency in self.memory.values():
            if dependency.param:
                ret[dependency.param] = dependency.id
        return ret

    def load_snapshot(self) -> Optional[Union[CodeSnapshot, dict, list]]:
        return self.code.service['snapshot'].load(serial_number=list(self.memory.keys()))

    def check_delivery(self) -> Union[bool, list]:
        return self.code.service['delivery'].check(serial_number=[k for k, v in self.memory.items() if v.param])

    def validate(self, snapshot: list) -> str:
        for s in snapshot:
            if s['state'] != 'TERMINATED':
                return 'WAITING'
        if len(self) != len(snapshot):
            return 'WAITING'
        if not self.check_delivery():
            return 'ERROR'
        return 'READY'

    def get_state(self) -> str:
        return self.validate(snapshot=self.load_snapshot())

    def __iter__(self) -> Iterator:
        return self.memory.__iter__()

    def keys(self) -> KeysView:
        return self.memory.keys()

    def values(self) -> ValuesView:
        return self.memory.values()

    def items(self) -> ItemsView:
        return self.memory.items()

    def __len__(self) -> int:
        return len(self.memory)
