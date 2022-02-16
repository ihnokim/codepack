import inspect
from collections.abc import Iterable, Callable
import dill
from codepack.config import Default
from codepack.base import CodeBase
import re
import ast
from collections import OrderedDict
from datetime import datetime
from codepack.dependency import Dependency, DependencyManager
from codepack.snapshot import CodeSnapshot


class Code(CodeBase):
    def __init__(self, function=None, source=None, id=None, serial_number=None, dependency=None,
                 config_path=None, delivery_service=None, snapshot_service=None, storage_service=None,
                 state=None, callback=None):
        super().__init__(id=id, serial_number=serial_number)
        self.function = None
        self.source = None
        self.description = None
        self.parents = None
        self.children = None
        self.dependency = None
        self.config_path = None
        self.service = None
        self.callback = None
        self.init_service(delivery_service=delivery_service,
                          snapshot_service=snapshot_service,
                          storage_service=storage_service,
                          config_path=config_path)
        self.set_function(function=function, source=source)
        if id is None:
            self.id = self.function.__name__
        self.init_linkage()
        self.init_dependency(dependency=dependency)
        self.register(callback=callback)
        self.update_state(state)

    def init_linkage(self):
        self.parents = dict()
        self.children = dict()

    def init_dependency(self, dependency=None):
        self.dependency = DependencyManager(code=self)
        if dependency:
            self.add_dependency(dependency=dependency)

    def init_service(self, delivery_service=None, snapshot_service=None, storage_service=None, config_path=None):
        self.service = dict()
        self.service['delivery'] =\
            delivery_service if delivery_service else Default.get_storage_instance('delivery', 'delivery_service',
                                                                                   config_path=config_path)
        self.service['snapshot'] =\
            snapshot_service if snapshot_service else Default.get_storage_instance('code_snapshot', 'snapshot_service',
                                                                                   config_path=config_path)
        self.service['storage'] =\
            storage_service if storage_service else Default.get_storage_instance('code', 'storage_service',
                                                                                 config_path=config_path)

    def register(self, callback):
        self.callback = callback

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
            tmp = self.service['storage'].load(id=self.id)
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

    def update_serial_number(self, serial_number):
        for c in self.children.values():
            if self.serial_number in c.dependency:
                d = c.dependency[self.serial_number]
                c.remove_dependency(self.serial_number)
                d.serial_number = serial_number
                c.add_dependency(d)
        self.serial_number = serial_number

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
                          self.dependency.get_args(),
                          self.get_state())
        else:
            ret += ')'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.print_args(),
                          self.dependency.get_args())

    def __str__(self):
        return self.get_info(state=False)  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def update_state(self, state, timestamp=None, args=None, kwargs=None):
        if state:
            snapshot = self.to_snapshot(args=args, kwargs=kwargs, timestamp=timestamp)
            snapshot['state'] = state
            self.service['snapshot'].save(snapshot)
            if self.callback:
                self.callback({'serial_number': self.serial_number, 'state': state})

    def get_state(self):
        ret = self.service['snapshot'].load(serial_number=self.serial_number, projection={'state'})
        if ret:
            return ret['state']
        else:
            return 'UNKNOWN'

    def send_result(self, item, timestamp=None):
        self.service['delivery'].send(id=self.id, serial_number=self.serial_number, item=item, timestamp=timestamp)

    def get_result(self, serial_number=None):
        serial_number = serial_number if serial_number else self.serial_number
        return self.service['delivery'].receive(serial_number=serial_number)

    def save(self, update=False):
        self.service['storage'].save(item=self, update=update)

    def receive(self, arg):
        self.assert_arg(arg)
        return Dependency(code=self, arg=arg)

    def assert_arg(self, arg):
        if arg and arg not in self.get_args():
            raise AssertionError("'%s' is not an argument of %s" % (arg, self.function))

    def add_dependency(self, dependency):
        self.dependency.add(dependency=dependency)

    def remove_dependency(self, serial_number):
        self.dependency.remove(serial_number)

    def __call__(self, *args, **kwargs):
        try:
            dependency_state = self.dependency.get_state()
            if dependency_state == 'RESOLVED':
                for dependency in self.dependency.values():
                    if dependency.arg and dependency.arg not in kwargs:
                        kwargs[dependency.arg] = self.get_result(serial_number=dependency.serial_number)
                self.update_state('RUNNING', args=args, kwargs=kwargs)
                ret = self.function(*args, **kwargs)
                now = datetime.now().timestamp()
                self.send_result(item=ret, timestamp=now)
                self.update_state('TERMINATED', args=args, kwargs=kwargs, timestamp=now)
                return ret
            elif dependency_state == 'PENDING':
                self.update_state('WAITING', args=args, kwargs=kwargs)
                return None
            elif dependency_state == 'ERROR':
                self.update_state('ERROR', args=args, kwargs=kwargs)
                return None
            else:
                raise NotImplementedError(dependency_state)  # pragma: no cover
        except Exception as e:
            self.update_state('ERROR', args=args, kwargs=kwargs)
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
