from codepack.utils.config.default import Default
from codepack.utils.config.config import Config
from codepack.base.code_base import CodeBase
from codepack.plugins.dependency import Dependency
from codepack.plugins.dependency_bag import DependencyBag
from codepack.plugins.callback import Callback
from codepack.plugins.state import State
from collections.abc import Iterable, Callable
from functools import partial
from typing import Any, TypeVar, Union, Optional
from queue import Queue
import codepack.utils.exceptions
import codepack.utils.functions
import inspect
import logging
import builtins
import os


CodeSnapshot = TypeVar('CodeSnapshot', bound='codepack.plugins.snapshots.code_snapshot.CodeSnapshot')
DeliveryService = TypeVar('DeliveryService', bound='codepack.plugins.delivery_service.DeliveryService')
SnapshotService = TypeVar('SnapshotService', bound='codepack.plugins.snapshot_service.SnapshotService')
StorageService = TypeVar('StorageService', bound='codepack.plugins.storage_service.StorageService')
# State = TypeVar('State', bound='codepack.plugins.state.State')


class Code(CodeBase):
    def __init__(self,
                 function: Optional[Callable] = None, source: Optional[str] = None, context: Optional[dict] = None,
                 id: Optional[str] = None, serial_number: Optional[str] = None,
                 dependency: Optional[Union[Dependency, dict, Iterable]] = None, config_path: Optional[str] = None,
                 delivery_service: Optional[DeliveryService] = None,
                 snapshot_service: Optional[SnapshotService] = None,
                 storage_service: Optional[StorageService] = None,
                 state: Optional[Union[State, str]] = None, callback: Optional[Union[list, Callable, Callback]] = None,
                 env: Optional[str] = None, image: Optional[str] = None, owner: Optional[str] = None,
                 decorator: Optional[Callable] = None,
                 log: bool = False) -> None:
        super().__init__(id=id, serial_number=serial_number, function=function, source=source, context=context)
        self.parents = None
        self.children = None
        self.dependency = None
        self.config_path = None
        self.service = None
        self.callback = dict()
        self.env = None
        self.image = None
        self.owner = None
        self.decorator = None
        self.log = log
        self.logger = None
        self.init_logger()
        self.init_service(delivery_service=delivery_service,
                          snapshot_service=snapshot_service,
                          storage_service=storage_service,
                          config_path=config_path)
        if id is None:
            self.id = self.function.__name__
        self.init_linkage()
        self.init_dependency(dependency=dependency)
        self.register_callback(callback=callback)
        self.set_decorator(decorator=decorator)
        self._set_str_attr(key='env', value=env)
        self._set_str_attr(key='image', value=image)
        self._set_str_attr(key='owner', value=owner)
        self.update_state(state)

    def init_logger(self) -> None:
        self.logger = logging.getLogger('code')
        if self.logger.level == logging.NOTSET or self.logger.level > logging.INFO:
            self.logger.setLevel(logging.INFO)

    def init_linkage(self) -> None:
        self.parents = dict()
        self.children = dict()

    def init_dependency(self, dependency: Optional[Union[Dependency, dict, Iterable]] = None) -> None:
        self.dependency = DependencyBag(code=self)
        if dependency:
            self.add_dependency(dependency=dependency)

    def init_service(self, delivery_service: Optional[DeliveryService] = None,
                     snapshot_service: Optional[DeliveryService] = None,
                     storage_service: Optional[DeliveryService] = None, config_path: Optional[str] = None) -> None:
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

    def _set_str_attr(self, key: str, value: str) -> None:
        if value is None or value == 'None' or value == 'null':
            _value = None
        else:
            _value = value
        setattr(self, key, _value)

    def register_callback(self, callback: Union[list, dict, Callable, Callback],
                          name: Optional[Union[list, str]] = None) -> None:
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
                raise TypeError(type(name))  # pragma: no cover
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
                raise TypeError(type(name))  # pragma: no cover
            self.callback[_name] = callback if isinstance(callback, Callback) else Callback(function=callback)
        elif isinstance(callback, dict):
            _callback = Callback.from_dict(callback)
            if isinstance(name, list):
                _name = name[0]
            elif isinstance(name, str):
                _name = name
            elif name is None:
                _name = _callback.id
            else:
                raise TypeError(type(name))  # pragma: no cover
            self.callback[_name] = _callback
        elif callback is None:
            pass
        else:
            raise TypeError(type(callback))  # pragma: no cover

    def set_decorator(self, decorator: Optional[Callable] = None) -> None:
        self.decorator = decorator

    def run_callback(self, state: Optional[Union[State, str]] = None, message: Optional[str] = None) -> None:
        for callback in self.callback.values():
            d = {'serial_number': self.serial_number, 'state': state if state else self.get_state()}
            if message:
                d['message'] = message
            callback(d)

    def _collect_linked_ids(self) -> set:
        ids = set()
        q = Queue()
        q.put(self)
        while not q.empty():
            code = q.get()
            ids.add(code.id)
            for c in {**code.children, **code.parents}.values():
                if c.id not in ids:
                    q.put(c)
        return ids

    def link(self, other: 'Code') -> None:
        linked_ids = self._collect_linked_ids()
        if other.id in linked_ids:
            raise ValueError("'%s' is already linked" % other.id)
        self.children[other.id] = other
        other.parents[self.id] = self
        if self.serial_number not in other.dependency:
            other.add_dependency(dependency=Dependency(code=other, serial_number=self.serial_number, id=self.id))

    def unlink(self, other: 'Code') -> None:
        other.parents.pop(self.id, None)
        self.children.pop(other.id, None)
        if self.serial_number in other.dependency:
            other.remove_dependency(self.serial_number)

    def __rshift__(self, other: Union['Code', Iterable]) -> Union['Code', Iterable]:
        if isinstance(other, self.__class__):
            self.link(other)
        elif isinstance(other, Iterable):
            for t in other:
                self.__rshift__(t)
        else:
            raise TypeError(type(other))  # pragma: no cover
        return other

    def __floordiv__(self, other: Union['Code', Iterable]) -> Union['Code', Iterable]:
        if isinstance(other, self.__class__):
            self.unlink(other)
        elif isinstance(other, Iterable):
            for t in other:
                self.__floordiv__(t)
        else:
            raise TypeError(type(other))  # pragma: no cover
        return other

    def update_serial_number(self, serial_number: str) -> None:
        for c in self.children.values():
            if self.serial_number in c.dependency:
                d = c.dependency[self.serial_number]
                c.remove_dependency(self.serial_number)
                d.serial_number = serial_number
                c.add_dependency(d)
        self.serial_number = serial_number

    def get_reserved_params(self) -> dict:
        return codepack.utils.functions.get_reserved_params(function=self.function)

    def print_params(self) -> str:
        signature = inspect.signature(self.function)
        return str(signature)

    @classmethod
    def blueprint(cls, s: str) -> str:
        ret = 'Code(id: {id}, function: {function}, params: {params}'
        for additional_item in ['receive', 'context', 'env', 'image', 'owner', 'state']:
            if ', %s:' % additional_item in s:
                ret += ', %s: {%s}' % (additional_item, additional_item)
        ret += ')'
        return ret

    def get_info(self, state: bool = True) -> str:
        ret = '%s(id: %s, function: %s, params: %s' % (self.__class__.__name__,
                                                       self.id, self.function.__name__,
                                                       self.print_params())
        dependent_params = self.dependency.get_params()
        if dependent_params:
            ret += ', receive: %s' % dependent_params
        for additional_item in ['context', 'env', 'image', 'owner']:
            item = getattr(self, additional_item)
            if item:
                ret += ', %s: %s' % (additional_item, item)
        if state:
            ret += ', state: %s' % self.get_state()
        ret += ')'
        return ret

    def __str__(self) -> str:
        return self.get_info(state=False)  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover

    def update_state(self, state: Union[State, str], timestamp: Optional[float] = None,
                     args: Optional[tuple] = None, kwargs: Optional[dict] = None, message: str = '') -> None:
        if state:
            snapshot = self.to_snapshot(args=args, kwargs=kwargs, timestamp=timestamp, message=message)
            snapshot['state'] = state
            self.service['snapshot'].save(snapshot)
            self.run_callback(state=state, message=message)

    def get_state(self) -> str:
        ret = self.service['snapshot'].load(serial_number=self.serial_number, projection={'state'})
        if ret:
            return ret['state']
        else:
            return 'UNKNOWN'

    def get_message(self) -> str:
        ret = self.service['snapshot'].load(serial_number=self.serial_number, projection={'message'})
        if ret:
            return ret['message']
        else:
            return ''

    def send_result(self, item: Any, timestamp: Optional[float] = None) -> None:
        self.service['delivery'].send(id=self.id, serial_number=self.serial_number, item=item, timestamp=timestamp)

    def get_result(self, serial_number: Optional[str] = None) -> Any:
        serial_number = serial_number if serial_number else self.serial_number
        return self.service['delivery'].receive(serial_number=serial_number)

    def save(self, update: bool = False) -> None:
        self.service['storage'].save(item=self, update=update)

    @classmethod
    def load(cls, id: Union[str, list], storage_service: Optional[StorageService] = None)\
            -> Optional[Union['Code', list]]:
        if storage_service is None:
            storage_service = Default.get_service('code', 'storage_service')
        return storage_service.load(id)

    @classmethod
    def remove(cls, id: Union[str, list], storage_service: Optional[StorageService] = None) -> None:
        if storage_service is None:
            storage_service = Default.get_service('code', 'storage_service')
        storage_service.remove(id=id)

    def receive(self, param: str) -> Dependency:
        self.assert_param(param)
        return Dependency(code=self, param=param)

    def assert_param(self, param: str) -> None:
        if param:
            fullargspec = inspect.getfullargspec(self.function)
            if fullargspec.varkw is None and param not in [*fullargspec.args, *fullargspec.kwonlyargs]:
                raise AssertionError("'%s' is not a valid parameter of %s" % (param, self.function))

    def add_dependency(self, dependency: Union[Dependency, dict, Iterable]) -> None:
        self.dependency.add(dependency=dependency)

    def remove_dependency(self, serial_number: str) -> None:
        self.dependency.remove(serial_number)

    def check_dependency(self) -> str:
        return self.dependency.get_state()

    def _log(self, msg: str, builtin_print: Callable) -> None:
        builtin_print(msg)
        self.logger.info(msg)

    def get_log_dir(self) -> str:
        return os.path.join(Config.get_log_dir(), self.id)

    def get_log_path(self) -> str:
        return os.path.join(self.get_log_dir(), '%s.log' % self.serial_number)

    def light(self, *args: Any, **kwargs: Any) -> Any:
        if self.decorator is not None:
            return self.decorator(self.function)(*args, **kwargs)
        else:
            return self.function(*args, **kwargs)

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        for k, v in self.context.items():
            if k not in kwargs:
                kwargs[k] = v
        for dependency in self.dependency.values():
            if dependency.param and dependency.param not in kwargs:
                kwargs[dependency.param] = self.get_result(serial_number=dependency.serial_number)
        if self.log:
            old_print = builtins.print
            log_dir = self.get_log_dir()
            log_path = self.get_log_path()
            codepack.utils.functions.mkdir(log_dir)
            handler = logging.FileHandler(log_path)
            formatter = logging.Formatter('%(asctime)s %(message)s')
            try:
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                builtins.print = partial(self._log, builtin_print=old_print)
                ret = self.light(*args, **kwargs)
            finally:
                builtins.print = old_print
                self.logger.removeHandler(handler)
                handler.close()
        else:
            ret = self.light(*args, **kwargs)
        self.send_result(item=ret)
        return ret

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ret = None
        try:
            state = self.check_dependency()
            self.update_state(state, args=args, kwargs=kwargs)
            if state == State.READY:
                self.update_state(State.RUNNING, args=args, kwargs=kwargs)
                ret = self._run(*args, **kwargs)
                self.update_state(State.TERMINATED, args=args, kwargs=kwargs)
        except codepack.utils.exceptions.UnknownState as e:
            self.update_state(State.UNKNOWN, args=args, kwargs=kwargs, message=str(e))
        except codepack.utils.exceptions.NewState as e:
            self.update_state(State.NEW, args=args, kwargs=kwargs, message=str(e))
        except codepack.utils.exceptions.ReadyState as e:
            self.update_state(State.READY, args=args, kwargs=kwargs, message=str(e))
        except codepack.utils.exceptions.WaitingState as e:
            self.update_state(State.WAITING, args=args, kwargs=kwargs, message=str(e))
        except codepack.utils.exceptions.RunningState as e:
            self.update_state(State.RUNNING, args=args, kwargs=kwargs, message=str(e))
        except codepack.utils.exceptions.TerminateState as e:
            self.update_state(State.TERMINATED, args=args, kwargs=kwargs, message=str(e))
        except codepack.utils.exceptions.ErrorState as e:
            self.update_state(State.ERROR, args=args, kwargs=kwargs, message=str(e))
        except Exception as e:
            self.update_state(State.ERROR, args=args, kwargs=kwargs, message=str(e))
            raise e
        return ret

    def to_dict(self) -> dict:
        d = dict()
        d['_id'] = self.id
        d['source'] = self.source
        d['description'] = self.description
        d['env'] = self.env
        d['image'] = self.image
        d['owner'] = self.owner
        d['context'] = self.context
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Code':
        return cls(id=d['_id'], source=d['source'],
                   env=d.get('env', None), image=d.get('image', None),
                   owner=d.get('owner', None), context=d.get('context', dict()))

    def to_snapshot(self, *args: Any, **kwargs: Any) -> CodeSnapshot:
        return self.service['snapshot'].convert_to_snapshot(self, *args, **kwargs)

    @classmethod
    def from_snapshot(cls, snapshot: CodeSnapshot) -> 'Code':
        return cls(id=snapshot['id'], serial_number=snapshot['serial_number'],
                   state=snapshot['state'], dependency=snapshot['dependency'], source=snapshot['source'],
                   env=snapshot['env'], image=snapshot['image'],
                   owner=snapshot['owner'], context=snapshot['context'],
                   callback=snapshot['callback'])
