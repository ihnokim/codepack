from codepack.config.default import Default
from codepack.base.code_base import CodeBase
from codepack.dependency.dependency import Dependency
from codepack.dependency.dependency_manager import DependencyManager
from codepack.snapshot import CodeSnapshot
from codepack.callback.callback import Callback
from collections.abc import Iterable, Callable
from functools import partial
from typing import Union, Callable as Function


class Code(CodeBase):
    def __init__(self, function: Function = None, source: str = None, id: str = None, serial_number: str = None,
                 dependency=None, config_path: str = None,
                 delivery_service=None, snapshot_service=None, storage_service=None,
                 state=None, callback: Union[list, Function[[dict], None], Callback] = None,
                 env: str = None, image: str = None, owner: str = None):
        super().__init__(id=id, serial_number=serial_number)
        self.parents = None
        self.children = None
        self.dependency = None
        self.config_path = None
        self.service = None
        self.callbacks = dict()
        self.env = None
        self.image = None
        self.owner = owner
        self.init_service(delivery_service=delivery_service,
                          snapshot_service=snapshot_service,
                          storage_service=storage_service,
                          config_path=config_path)
        self.set_function(function=function, source=source)
        if id is None:
            self.id = self.function.__name__
        self.init_linkage()
        self.init_dependency(dependency=dependency)
        self.register_callback(callback=callback)
        self.set_str_attr(key='env', value=env)
        self.set_str_attr(key='image', value=image)
        self.set_str_attr(key='owner', value=owner)
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
            delivery_service if delivery_service else Default.get_service('delivery', 'delivery_service',
                                                                          config_path=config_path)
        self.service['snapshot'] =\
            snapshot_service if snapshot_service else Default.get_service('code_snapshot', 'snapshot_service',
                                                                          config_path=config_path)
        self.service['storage'] =\
            storage_service if storage_service else Default.get_service('code', 'storage_service',
                                                                        config_path=config_path)

    def set_str_attr(self, key: str, value: str):
        if value is None or value == 'None' or value == 'null':
            _value = None
        else:
            _value = value
        setattr(self, key, _value)

    def register_callback(self, callback: Union[list, Function[[dict], None], Callback], name: Union[list, str] = None):
        if isinstance(callback, list):
            if isinstance(name, list):
                if len(callback) != len(name):
                    raise IndexError('len(callback) != len(name): %s != %s' % (len(callback), len(name)))
                else:
                    for c, n in zip(callback, name):
                        self.register_callback(callback=c, name=n)
            elif isinstance(name, str):
                if len(callback) == 1:
                    self.register_callback(callback=callback[0], name=name)
                else:
                    raise IndexError('len(callback) != len(name): %s != 1' % len(callback))
            elif name is None:
                for c in callback:
                    self.register_callback(callback=c)
            else:
                raise TypeError(type(name))
        elif isinstance(callback, Callable):
            if isinstance(name, list):
                _name = name[0]
            elif isinstance(name, str):
                _name = name
            elif name is None:
                if isinstance(callback, partial):
                    _name = callback.func.__name__
                elif isinstance(callback, Callback):
                    _name = callback.function.__name__
                else:
                    _name = callback.__name__
            else:
                raise TypeError(type(name))
            self.callbacks[_name] = callback
        elif callback is None:
            pass
        else:
            raise TypeError(type(callback))

    def run_callback(self, state: str = None, message: str = None):
        for callback in self.callbacks.values():
            d = {'serial_number': self.serial_number, 'state': state if state else self.get_state()}
            if message:
                d['message'] = message
            callback(d)
    
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
        return super().get_args(function=self.function)

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

    @classmethod
    def blueprint(cls, s):
        ret = 'Code(id: {id}, function: {function}, args: {args}, receive: {receive}'
        for additional_item in ['env', 'image', 'owner', 'state']:
            if ', %s:' % additional_item in s:
                ret += ', %s: {%s}' % (additional_item, additional_item)
        ret += ')'
        return ret

    def get_info(self, state=True):
        ret = '%s(id: %s, function: %s, args: %s, receive: %s' % (self.__class__.__name__,
                                                                  self.id, self.function.__name__,
                                                                  self.print_args(), self.dependency.get_args())
        for additional_item in ['env', 'image', 'owner']:
            item = getattr(self, additional_item)
            if item:
                ret += ', %s: %s' % (additional_item, item)
        if state:
            ret += ', state: %s' % self.get_state()
        ret += ')'
        return ret

    def __str__(self):
        return self.get_info(state=False)  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def update_state(self, state: str, timestamp: float = None,
                     args: tuple = None, kwargs: dict = None, message: str = ''):
        if state:
            snapshot = self.to_snapshot(args=args, kwargs=kwargs, timestamp=timestamp, message=message)
            snapshot['state'] = state
            self.service['snapshot'].save(snapshot)
            self.run_callback(state=state, message=message)

    def get_state(self):
        ret = self.service['snapshot'].load(serial_number=self.serial_number, projection={'state'})
        if ret:
            return ret['state']
        else:
            return 'UNKNOWN'

    def get_message(self):
        ret = self.service['snapshot'].load(serial_number=self.serial_number, projection={'message'})
        if ret:
            return ret['message']
        else:
            return ''

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

    def check_dependency(self):
        return self.dependency.get_state()

    def _run(self, *args, **kwargs):
        for dependency in self.dependency.values():
            if dependency.arg and dependency.arg not in kwargs:
                kwargs[dependency.arg] = self.get_result(serial_number=dependency.serial_number)
        ret = self.function(*args, **kwargs)
        self.send_result(item=ret)
        return ret

    def __call__(self, *args, **kwargs):
        ret = None
        try:
            state = self.check_dependency()
            self.update_state(state, args=args, kwargs=kwargs)
            if state == 'READY':
                self.update_state('RUNNING', args=args, kwargs=kwargs)
                ret = self._run(*args, **kwargs)
                self.update_state('TERMINATED', args=args, kwargs=kwargs)
        except Exception as e:
            self.update_state('ERROR', args=args, kwargs=kwargs, message=str(e))
            raise e
        return ret

    def to_dict(self):
        d = dict()
        d['_id'] = self.id
        d['source'] = self.source
        d['description'] = self.description
        d['env'] = self.env
        d['image'] = self.image
        d['owner'] = self.owner
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['_id'], source=d['source'],
                   env=d.get('env', None), image=d.get('image', None), owner=d.get('owner', None))

    def to_snapshot(self, *args, **kwargs):
        return CodeSnapshot(self, *args, **kwargs)

    @classmethod
    def from_snapshot(cls, snapshot):
        return cls(id=snapshot['id'], serial_number=snapshot['serial_number'],
                   state=snapshot['state'], dependency=snapshot['dependency'], source=snapshot['source'],
                   env=snapshot['env'], image=snapshot['image'], owner=snapshot['owner'])
