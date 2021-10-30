import inspect
from copy import copy
from collections.abc import Iterable, Callable
import dill
from codepack.state import State
from codepack.delivery_service import DeliveryService
from codepack.state_manager import StateManager
from codepack.abc import CodeBase
from codepack.utils import get_config
import os
import re
import ast
from collections import OrderedDict


class Code(CodeBase):
    def __init__(self, function=None, source=None, id=None,
                 mongodb=None,
                 store_db=None, store_collection=None,
                 cache_db=None, cache_collection=None,
                 state_db=None, state_collection=None, config_filepath=None, serial_number=None):
        super().__init__(serial_number=serial_number)
        self.mongodb = None
        self.store_db = None
        self.store_collection = None
        self.cache_db = None
        self.cache_collection = None
        self.state_db = None
        self.state_collection = None
        self.config_filepath = None
        self.state = None
        self.function = None
        self.source = None
        self.description = None
        self.parents = None
        self.children = None
        self.order_list = None
        self.delivery_service = None
        self.state_manager = None
        self.online = False
        self.link_to_mongodb(mongodb, store_db=store_db, store_collection=store_collection,
                             cache_db=cache_db, cache_collection=cache_collection,
                             state_db=state_db, state_collection=state_collection, config_filepath=config_filepath)
        self.set_function(function=function, source=source)
        if id is None:
            self.id = self.function.__name__
        else:
            self.id = id
        self.init()

    def link_to_mongodb(self, mongodb, store_db=None, store_collection=None, cache_db=None, cache_collection=None,
                        state_db=None, state_collection=None, config_filepath=None):
        self.online = False
        self.mongodb = mongodb
        self.store_db = store_db
        self.store_collection = store_collection
        self.cache_db = cache_db
        self.cache_collection = cache_collection
        self.state_db = state_db
        self.state_collection = state_collection
        self.config_filepath = config_filepath
        if self.mongodb:
            self.online = True
            if not self.config_filepath:
                self.config_filepath = os.environ.get('CODEPACK_CONFIG_FILEPATH', None)
            if self.config_filepath:
                tmp_config = dict()
                for section in ['store', 'cache', 'state']:
                    _section = 'code' if section == 'store' else section
                    tmp_config[section] = get_config(self.config_filepath, section=_section)
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
        if self.delivery_service:
            self.delivery_service.link_to_mongodb(mongodb=mongodb, db=self.cache_db, collection=self.cache_collection,
                                                  online=self.online)

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
                                                db=self.cache_db, collection=self.cache_collection,
                                                online=self.online)
        self.state_manager = StateManager(mongodb=self.mongodb, db=self.state_db, collection=self.state_collection,
                                          online=self.online)
        self.update_state(State.NEW)

    def receive(self, arg):
        self.delivery_service.order(name=arg)
        return self.delivery_service.get_order(name=arg)

    def __rshift__(self, other):
        if isinstance(other, self.__class__):
            self.children[other.id] = other
            other.parents[self.id] = self
            for order in other.delivery_service:
                if order.sender == self.id:
                    order.invoice_number = self.serial_number
        elif isinstance(other, Iterable):
            for t in other:
                self.__rshift__(t)
        else:
            raise TypeError(type(other))
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
                          self.delivery_service.get_senders(),
                          self.get_state())
        else:
            ret += ')'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.print_args(),
                          self.delivery_service.get_senders())

    def __str__(self):
        return self.get_info(state=True)

    def __repr__(self):
        return self.__str__()

    def update_state(self, state):
        self.state_manager.update(self, state)

    def get_state(self):
        return self.state_manager.get(self)

    def __call__(self, *args, **kwargs):
        self.update_state(State.RUNNING)
        try:
            for order in self.delivery_service:
                if order.sender is not None and order.name not in kwargs:
                    assert order.sender in self.parents.keys(), "cannot find the sender '%s'" % order.sender
                    assert self.parents[order.sender].serial_number == order.invoice_number,\
                        "linkage between '%s' and '%s' is corrupted" % (self.parents[order.sender].id, self.id)
                    if self.parents[order.sender].online:
                        item = self.delivery_service.receive(order.invoice_number)
                    else:
                        item = self.parents[order.sender].delivery_service.tmp_storage
                    kwargs[order.name] = item
            ret = self.function(*args, **kwargs)
            self.delivery_service.send(sender=self, item=ret)
            self.update_state(State.TERMINATED)
        except Exception as e:
            self.update_state(State.ERROR)
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
