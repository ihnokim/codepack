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


class Code(CodeBase):
    def __init__(self, function=None, source=None, id=None, serial_number=None, dependency=None,
                 config_path=None, delivery_service=None, state_manager=None, storage_service=None):
        super().__init__(id=id, serial_number=serial_number)
        self.function = None
        self.source = None
        self.description = None
        self.parents = None
        self.children = None
        self.dependency = None
        self.config_path = None
        self.service = None
        self.set_function(function=function, source=source)
        if id is None:
            self.id = self.function.__name__
        self.init(dependency=dependency, config_path=config_path,
                  delivery_service=delivery_service, state_manager=state_manager, storage_service=storage_service)

    def init(self, dependency=None, config_path=None,
             delivery_service=None, state_manager=None, storage_service=None):
        self.parents = dict()
        self.children = dict()
        self.init_dependency(dependency=dependency)
        self.init_service(delivery_service=delivery_service,
                          state_manager=state_manager,
                          storage_service=storage_service,
                          config_path=config_path)
        self.update_state('NEW')

    def init_dependency(self, dependency=None):
        self.dependency = dict()
        if dependency:
            self.add_dependency(dependency=dependency)

    def init_service(self, delivery_service=None, state_manager=None, storage_service=None, config_path=None):
        self.service = dict()
        self.service['delivery_service'] =\
            delivery_service if delivery_service else DefaultServicePack.get_default_delivery_service(config_path=config_path)
        self.service['state_manager'] =\
            state_manager if state_manager else DefaultServicePack.get_default_state_manager(config_path=config_path)
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
        assert function or source, "either 'function' or 'source' should not be None"
        if source:
            source = source.strip()
            self.function = self.get_function(source)
            self.source = source
        elif function:
            self.function = function
            self.source = self.get_source(self.function)
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
        return self.get_info(state=True)

    def __repr__(self):
        return self.__str__()

    def update_state(self, state, update_time=None):
        self.service['state_manager'].set(id=self.id, serial_number=self.serial_number, state=state,
                                          update_time=update_time, dependency=self.dependency)

    def get_state(self):
        return self.service['state_manager'].get(serial_number=self.serial_number)

    def send_result(self, item, send_time=None):
        self.service['delivery_service'].send(sender=self.id, invoice_number=self.serial_number, item=item, send_time=send_time)

    def get_result(self, serial_number):
        return self.service['delivery_service'].receive(invoice_number=serial_number)

    def save(self, *args, **kwargs):
        self.service['storage_service'].save(self, *args, **kwargs)

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

    def get_dependency_state_info(self):
        ret = dict()
        states = self.service['state_manager'].check(serial_number=list(self.dependency.keys()))
        for state in states:
            ret[state['_id']] = state
        return ret

    def get_dependency_cache_info(self):
        ret = dict()
        caches = self.service['delivery_service'].check(invoice_number=[k for k, v in self.dependency.items() if v.arg])
        for cache in caches:
            ret[cache['_id']] = cache
        return ret

    def check_dependency_termination(self, states):
        if len(self.dependency) != len(states):
            return False
        for state in states.values():
            if state['state'] != 'TERMINATED':
                return False
        return True

    def validate_dependency_result(self, states, caches):
        if len([k for k, v in self.dependency.items() if v.arg]) != len(caches):
            return False
        for cache in caches.values():
            if cache['_id'] not in states:
                return False
            elif 'send_time' not in cache or 'update_time' not in states[cache['_id']]:
                return False
            elif cache['send_time'] != states[cache['_id']]['update_time']:
                return False
        return True

    def check_dependency(self):
        states = self.get_dependency_state_info()
        if not self.check_dependency_termination(states=states):
            return False
        caches = self.get_dependency_cache_info()
        if not self.validate_dependency_result(states=states, caches=caches):
            return False
        return True

    def __call__(self, *args, **kwargs):
        try:
            self.update_state('READY')
            if self.check_dependency():
                for dependency in self.dependency.values():
                    if dependency.arg and dependency.arg not in kwargs:
                        kwargs[dependency.arg] = self.get_result(serial_number=dependency.serial_number)
                self.update_state('RUNNING')
                ret = self.function(*args, **kwargs)
                now = datetime.now().timestamp()
                self.send_result(item=ret, send_time=now)
                self.update_state('TERMINATED', update_time=now)
                return ret
            else:
                self.update_state('WAITING')
                return None
        except Exception as e:
            self.update_state('ERROR')
            raise e

    def to_dict(self, *args, **kwargs):
        d = dict()
        d['_id'] = self.id
        d['source'] = self.source
        d['description'] = self.description
        return d

    @classmethod
    def from_dict(cls, d, *args, **kwargs):
        return cls(id=d['_id'], source=d['source'], *args, **kwargs)
