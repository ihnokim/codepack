import inspect
from copy import copy
from collections.abc import Iterable, Callable
import dill
from codepack.state import State
from codepack.delivery import Delivery, DeliveryService
from codepack.abc import CodeBase
from codepack.utils import get_config
import os
import re
import ast


class Code(CodeBase):
    def __init__(self, function=None, source=None, id=None,
                 mongodb=None,
                 store_db=None, store_collection=None,
                 cache_db=None, cache_collection=None,
                 state_db=None, state_collection=None, config_filepath=None):
        super().__init__()
        self.mongodb = mongodb
        self.store_db = store_db
        self.store_collection = store_collection
        self.cache_db = cache_db
        self.cache_collection = cache_collection
        self.state_db = state_db
        self.state_collection = state_collection
        self.online = False
        if self.mongodb:
            self.online = True
            if not config_filepath:
                config_filepath = os.environ.get('CODEPACK_CONFIG_FILEPATH', None)
            if config_filepath:
                tmp_config = dict()
                for section in ['store', 'cache', 'state']:
                    _section = 'code' if section == 'store' else section
                    tmp_config[section] = get_config(config_filepath, section=_section)
                    for key in ['db', 'collection']:
                        attr = section + '_' + key
                        if not getattr(self, attr):
                            setattr(self, attr, tmp_config[section][key])
            else:
                for section in ['store', 'cache', 'state']:
                    for key in ['db', 'collection']:
                        attr = section + '_' + key
                        if not getattr(self, attr):
                            raise AssertionError(self._config_assertion_error_message(attr))
        self.state = None
        self.function = None
        self.source = None
        self.description = None
        self.parents = None
        self.children = None
        self.delivery_service = None
        self.state_manager = None
        self.set_function(function=function, source=source)
        if id is None:
            self.id = self.function.__name__
        else:
            self.id = id
        self.init()

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
            except Exception:
                pass
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

    def init(self):
        self.parents = dict()
        self.children = dict()
        self.delivery_service = DeliveryService(mongodb=self.mongodb,
                                                db=self.cache_db, collection=self.cache_collection, online=self.online)
        # self.state_manager = StateManager()
        for arg in self.get_args():
            self.delivery_service.request(arg)
        self.update_state(State.NEW)

    def get_ready(self, return_deliveries=False):
        self.update_state(State.READY)
        if return_deliveries:
            self.delivery_service.return_deliveries()

    @staticmethod
    def return_delivery(sender, delivery):
        if isinstance(delivery, Iterable):
            for d in delivery:
                if d.sender == sender:
                    d.send(None)
        elif isinstance(delivery, Delivery):
            if delivery.sender == sender:
                delivery.send(None)
        else:
            raise TypeError(type(delivery))

    def receive(self, arg):
        return self.delivery_service.inquire(arg)

    def __rshift__(self, other):
        if isinstance(other, self.__class__):
            self.children[other.id] = other
            other.parents[self.id] = self
            other.delivery_service.return_deliveries(sender=self.id)
            self.get_ready(return_deliveries=True)
        elif isinstance(other, Iterable):
            for t in other:
                self.__rshift__(t)
        else:
            raise TypeError(type(other))
        return other

    def get_args(self):
        return inspect.getfullargspec(self.function).args

    def get_info(self, state=True):
        ret = '%s(id: %s, function: %s, args: %s, receive: %s'
        if state:
            ret += ', state: %s)'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.get_args(),
                          self.delivery_service.get_senders(),
                          self.state)
        else:
            ret += ')'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.get_args(),
                          self.delivery_service.get_senders())

    def __str__(self):
        return self.get_info(state=True)

    def __repr__(self):
        return self.__str__()

    def update_state(self, state):
        self.state = state

    def get_state(self):
        return self.state

    def __call__(self, *args, **kwargs):
        self.update_state(State.RUNNING)
        try:
            for delivery in self.delivery_service:
                if delivery.sender is not None and delivery.name not in kwargs:
                    kwargs[delivery.name] = delivery.item
            ret = self.function(*args, **kwargs)
            for c in self.children.values():
                c.delivery_service.send_deliveries(sender=self.id, item=ret)

            self.update_state(State.TERMINATED)
        except Exception as e:
            self.update_state(State.READY)
            raise Exception(e)
        return ret

    def clone(self):
        tmp = copy(self)
        tmp.init()
        return tmp

    def to_dict(self):
        d = dict()
        d['_id'] = self.id
        d['source'] = self.source
        d['description'] = self.description
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['_id'], source=d['source'])

    def to_db(self, db=None, collection=None, config=None, ssh_config=None, mongodb=None, **kwargs):
        if not config and not mongodb:
            if self.mongodb:
                mongodb = self.mongodb
        if not db:
            db = self.store_db
        if not collection:
            collection = self.store_collection
        super().to_db(db=db, collection=collection, config=config, ssh_config=ssh_config, mongodb=mongodb, **kwargs)
