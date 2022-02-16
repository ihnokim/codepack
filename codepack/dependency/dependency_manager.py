from codepack.storage import MemoryStorage
from codepack.dependency.dependency import Dependency
from codepack.dependency.dependency_state import DependencyState
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

    def validate_snapshot(self, snapshot):
        for s in snapshot:
            if s['state'] == 'ERROR':
                return DependencyState.ERROR
            elif s['state'] != 'TERMINATED':
                return DependencyState.PENDING
        if len(self) != len(snapshot):
            return DependencyState.PENDING
        return DependencyState.RESOLVED

    def validate_delivery(self, snapshot, delivery):
        if len(self.get_args()) != len(delivery):
            return DependencyState.PENDING
        s = {x['_id']: x for x in snapshot}
        for d in delivery:
            serial_number = d['_id']
            if serial_number not in s:
                return DependencyState.PENDING
            elif 'timestamp' not in d or 'timestamp' not in s[serial_number]:
                return DependencyState.PENDING
            elif d['timestamp'] != s[serial_number]['timestamp']:
                return DependencyState.PENDING
        return DependencyState.RESOLVED

    def get_state(self):
        snapshot = self.load_snapshot()
        dependency_state = self.validate_snapshot(snapshot=snapshot)
        if dependency_state != DependencyState.RESOLVED:
            return dependency_state
        delivery = self.check_delivery()
        return self.validate_delivery(snapshot=snapshot, delivery=delivery)

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
