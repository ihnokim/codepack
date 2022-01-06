import inspect
from collections.abc import Iterable, Callable
import dill
from codepack.service import DefaultServicePack
from codepack.abc import CodeBase
import re
import ast
from collections import OrderedDict
from datetime import datetime
from codepack.utils.dependency import Dependency
from codepack.utils.dependency_state import DependencyState
from codepack.utils.snapshot import CodeSnapshot


class Code(CodeBase):
    def __init__(self, function=None, source=None, id=None, serial_number=None, dependency=None,
                 config_path=None, delivery_service=None, snapshot_service=None, storage_service=None, state=None):
        super().__init__(id=id, serial_number=serial_number)
        self.function = None
        self.source = None
        self.description = None
        self.parents = None
        self.children = None
        self.dependency = None
        self.config_path = None
        self.service = None
        self.init_service(delivery_service=delivery_service,
                          snapshot_service=snapshot_service,
                          storage_service=storage_service,
                          config_path=config_path)
        self.set_function(function=function, source=source)
        if id is None:
            self.id = self.function.__name__
        self.init_linkage()
        self.init_dependency(dependency=dependency)
        self.update_state(state)

    def init_linkage(self):
        self.parents = dict()
        self.children = dict()

    def init_dependency(self, dependency=None):
        self.dependency = dict()
        if dependency:
            self.add_dependency(dependency=dependency)

    def init_service(self, delivery_service=None, snapshot_service=None, storage_service=None, config_path=None):
        self.service = dict()
        self.service['delivery_service'] =\
            delivery_service if delivery_service else DefaultServicePack.get_default_delivery_service(config_path=config_path)
        self.service['snapshot_service'] =\
            snapshot_service if snapshot_service else DefaultServicePack.get_default_code_snapshot_service(config_path=config_path)
        self.service['storage_service'] =\
            storage_service if storage_service else DefaultServicePack.get_default_code_storage_service(obj=self.__class__,
                                                                                                        config_path=config_path)

    @staticmethod
    def get_source(function):
        assert isinstance(function, Callable), "'function' should be an instance of Callable"
        assert function.__name__ != '<lambda>', "Invalid function '<lambda>'"
        assert hasattr(function, '__code__'), "'function' should have an attribute '__code__'"
        assert function.__code__.co_filename != '<string>', "'function' should not be defined in <string>"
        source = None
        for test in [inspect.getsource, dill.source.getsource]:
            try:
                source = test(function)
            except Exception:  # pragma: no cover
                pass  # pragma: no cover
            if source is not None:
                break
        return source

    @staticmethod
    def get_function(source):
        pat = re.compile('^(\\s*def\\s.+[(].*[)].*[:])|(\\s*async\\s+def\\s.+[(].*[)].*[:])')
        assert pat.match(source), "'source' is not a function"
        tree = ast.parse(source, mode='exec')
        n_function = sum(isinstance(exp, ast.FunctionDef) for exp in tree.body)
        # needs to count all other instances, and assert that there is only one FunctionDef
        assert n_function == 1, "'source' should contain only one function."
        namespace = dict()
        # code = compile(tree, filename='blah', mode='exec')
        exec(source, namespace)
        return namespace[tree.body[0].name]
    
    def set_function(self, function=None, source=None):
        if source:
            source = source.strip()
            self.function = self.get_function(source)
            self.source = source
        elif function:
            self.function = function
            self.source = self.get_source(self.function)
        elif self.id:
            tmp = self.service['storage_service'].load(id=self.id)
            self.function = self.get_function(tmp.source)
            self.source = tmp.source
        else:
            raise AssertionError("either 'function' or 'source' should not be None")
        self.description = self.function.__doc__.strip() if self.function.__doc__ is not None else str()

    def link(self, other):
        self.children[other.id] = other
        other.parents[self.id] = self
        if self.serial_number not in other.dependency:
            other.add_dependency(dependency=Dependency(code=other, serial_number=self.serial_number, id=self.id))

    def unlink(self, other):
        other.parents.pop(self.id, None)
        self.children.pop(other.id, None)
        if self.serial_number in other.dependency:
            other.remove_dependency(self.serial_number)

    def __rshift__(self, other):
        if isinstance(other, self.__class__):
            self.link(other)
        elif isinstance(other, Iterable):
            for t in other:
                self.__rshift__(t)
        else:
            raise TypeError(type(other))  # pragma: no cover
        return other

    def __floordiv__(self, other):
        if isinstance(other, self.__class__):
            self.unlink(other)
        elif isinstance(other, Iterable):
            for t in other:
                self.__floordiv__(t)
        else:
            raise TypeError(type(other))  # pragma: no cover
        return other

    def get_args(self):
        ret = OrderedDict()
        argspec = inspect.getfullargspec(self.function)
        args = argspec.args
        defaults = dict(zip(args[-len(argspec.defaults):], argspec.defaults)) if argspec.defaults else dict()
        for arg in args:
            if arg in defaults:
                ret[arg] = defaults[arg]
            else:
                ret[arg] = None
        return ret

    def print_args(self):
        ret = '('
        for i, (arg, value) in enumerate(self.get_args().items()):
            if i:
                ret += ', '
            ret += arg
            if value:
                ret += '=%s' % value
        ret += ')'
        return ret

    def get_info(self, state=True):
        ret = '%s(id: %s, function: %s, args: %s, receive: %s'
        if state:
            ret += ', state: %s)'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.print_args(),
                          self.get_dependent_args(),
                          self.get_state())
        else:
            ret += ')'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.print_args(),
                          self.get_dependent_args())

    def __str__(self):
        return self.get_info(state=False)  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def update_state(self, state, timestamp=None, args=None, kwargs=None):
        snapshot = self.to_snapshot(args=args, kwargs=kwargs, timestamp=timestamp)
        snapshot['state'] = state
        self.service['snapshot_service'].save(snapshot)

    def get_state(self):
        ret = self.service['snapshot_service'].load(serial_number=self.serial_number, projection={'state'})
        if ret:
            return ret['state']
        else:
            return 'UNKNOWN'

    def send_result(self, item, timestamp=None):
        self.service['delivery_service'].send(id=self.id, serial_number=self.serial_number, item=item, timestamp=timestamp)

    def get_result(self, serial_number=None):
        serial_number = serial_number if serial_number else self.serial_number
        return self.service['delivery_service'].receive(serial_number=serial_number)

    def save(self):
        self.service['storage_service'].save(item=self)

    def receive(self, arg):
        self.assert_arg(arg)
        return Dependency(code=self, arg=arg)

    def assert_arg(self, arg):
        if arg and arg not in self.get_args():
            raise AssertionError("'%s' is not an argument of %s" % (arg, self.function))

    def get_dependent_args(self):
        ret = dict()
        for dependency in self.dependency.values():
            if dependency.arg:
                ret[dependency.arg] = dependency.id
        return ret

    def add_dependency(self, dependency):
        if isinstance(dependency, Dependency):
            self.assert_arg(dependency.arg)
            self.dependency[dependency.serial_number] = dependency
        elif isinstance(dependency, dict):
            self.assert_arg(dependency['arg'])
            self.dependency[dependency['serial_number']] = Dependency.from_dict(d=dependency)
            self.dependency[dependency['serial_number']].bind(self)
        elif isinstance(dependency, Iterable):
            for d in dependency:
                self.add_dependency(d)
        else:
            raise TypeError(type(dependency))  # pragma: no cover

    def remove_dependency(self, serial_number):
        self.dependency.pop(serial_number, None)

    def get_dependent_snapshots(self):
        ret = dict()
        snapshots = self.service['snapshot_service'].load(serial_number=list(self.dependency.keys()))
        for snapshot in snapshots:
            ret[snapshot['_id']] = snapshot
        return ret

    def get_dependency_cache_info(self):
        ret = dict()
        caches = self.service['delivery_service'].check(serial_number=[k for k, v in self.dependency.items() if v.arg])
        for cache in caches:
            ret[cache['_id']] = cache
        return ret

    def check_dependency_state(self, snapshots):
        for snapshot in snapshots.values():
            if snapshot['state'] == 'ERROR':
                return DependencyState.ERROR
            elif snapshot['state'] != 'TERMINATED':
                return DependencyState.NOT_READY
        if len(self.dependency) != len(snapshots):
            return DependencyState.NOT_READY
        return DependencyState.READY

    def validate_dependency_result(self, snapshots, caches):
        if len([k for k, v in self.dependency.items() if v.arg]) != len(caches):
            return DependencyState.NOT_READY
        for cache in caches.values():
            if cache['_id'] not in snapshots:
                return DependencyState.NOT_READY
            elif 'timestamp' not in cache or 'timestamp' not in snapshots[cache['_id']]:
                return DependencyState.NOT_READY
            elif cache['timestamp'] != snapshots[cache['_id']]['timestamp']:
                return DependencyState.NOT_READY
        return DependencyState.READY

    def check_dependency(self):
        snapshots = self.get_dependent_snapshots()
        dependency_state = self.check_dependency_state(snapshots=snapshots)
        if dependency_state != 'READY':
            return dependency_state
        caches = self.get_dependency_cache_info()
        return self.validate_dependency_result(snapshots=snapshots, caches=caches)

    def __call__(self, *args, **kwargs):
        try:
            dependency_state = self.check_dependency()
            if dependency_state == 'READY':
                for dependency in self.dependency.values():
                    if dependency.arg and dependency.arg not in kwargs:
                        kwargs[dependency.arg] = self.get_result(serial_number=dependency.serial_number)
                self.update_state('RUNNING')
                ret = self.function(*args, **kwargs)
                now = datetime.now().timestamp()
                self.send_result(item=ret, timestamp=now)
                self.update_state('TERMINATED', timestamp=now)
                return ret
            elif dependency_state == 'NOT_READY':
                self.update_state('WAITING', args=args, kwargs=kwargs)
                return None
            elif dependency_state == 'ERROR':
                self.update_state('ERROR', args=args, kwargs=kwargs)
                return None
            else:
                raise NotImplementedError(dependency_state)  # pragma: no cover
        except Exception as e:
            self.update_state('ERROR')
            raise e

    def to_dict(self):
        d = dict()
        d['_id'] = self.id
        d['source'] = self.source
        d['description'] = self.description
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['_id'], source=d['source'])

    def to_snapshot(self, *args, **kwargs):
        return CodeSnapshot(self, *args, **kwargs)

    @classmethod
    def from_snapshot(cls, snapshot):
        return cls(id=snapshot['id'], serial_number=snapshot['serial_number'],
                   state=snapshot['state'], dependency=snapshot['dependency'], source=snapshot['source'])