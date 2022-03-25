from codepack.storage.memory_storage import MemoryStorage
from codepack.dependency.dependency import Dependency
from collections.abc import Iterable


class DependencyManager(MemoryStorage, Iterable):
    def __init__(self, code, item_type=Dependency):
        MemoryStorage.__init__(self, item_type=item_type)
        self.code = code

    def add(self, dependency):
        if isinstance(dependency, Dependency):
            self.code.assert_arg(dependency.arg)
            self.memory[dependency.serial_number] = dependency
        elif isinstance(dependency, dict):
            self.code.assert_arg(dependency['arg'])
            self.memory[dependency['serial_number']] = Dependency.from_dict(d=dependency)
            self.memory[dependency['serial_number']].bind(self.code)
        elif isinstance(dependency, Iterable):
            for d in dependency:
                self.add(d)
        else:
            raise TypeError(type(dependency))  # pragma: no cover

    def __getitem__(self, item):
        return self.memory[item]

    def remove(self, serial_number):
        self.memory.pop(serial_number, None)

    def get_args(self):
        ret = dict()
        for dependency in self.memory.values():
            if dependency.arg:
                ret[dependency.arg] = dependency.id
        return ret

    def load_snapshot(self):
        return self.code.service['snapshot'].load(serial_number=list(self.memory.keys()))

    def check_delivery(self):
        return self.code.service['delivery'].check(serial_number=[k for k, v in self.memory.items() if v.arg])

    def validate(self, snapshot):
        for s in snapshot:
            if s['state'] != 'TERMINATED':
                return 'WAITING'
        if len(self) != len(snapshot):
            return 'WAITING'
        if not self.check_delivery():
            return 'ERROR'
        return 'READY'

    def get_state(self):
        return self.validate(snapshot=self.load_snapshot())

    def __iter__(self):
        return self.memory.__iter__()

    def keys(self):
        return self.memory.keys()

    def values(self):
        return self.memory.values()

    def items(self):
        return self.memory.items()

    def __len__(self):
        return len(self.memory)
