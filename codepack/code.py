import inspect
from copy import copy
from collections.abc import Iterable, Callable
import dill
from codepack.state import State
from codepack.delivery import Delivery, DeliveryService
from codepack.abc import CodeBase
import re
import ast


class Code(CodeBase):
    def __init__(self, function=None, source=None, id=None):
        super().__init__()
        self.status = None
        self.function = None
        self.source = None
        self.description = None
        self.parents = None
        self.children = None
        self.delivery_service = None
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
        self.delivery_service = DeliveryService()
        for arg in self.get_args():
            self.delivery_service.request(arg)
        self.update_status(State.NEW)

    def get_ready(self, return_deliveries=False):
        self.update_status(State.READY)
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

    def get_info(self, status=True):
        ret = '%s(id: %s, function: %s, args: %s, receive: %s'
        if status:
            ret += ', status: %s)'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.get_args(),
                          self.delivery_service.get_senders(),
                          self.status)
        else:
            ret += ')'
            return ret % (self.__class__.__name__, self.id, self.function.__name__,
                          self.get_args(),
                          self.delivery_service.get_senders())

    def __str__(self):
        return self.get_info(status=True)

    def __repr__(self):
        return self.__str__()

    def update_status(self, status):
        self.status = status

    def __call__(self, *args, **kwargs):
        self.update_status(State.RUNNING)
        try:
            for delivery in self.delivery_service:
                if delivery.sender is not None and delivery.name not in kwargs:
                    kwargs[delivery.name] = delivery.item
            ret = self.function(*args, **kwargs)
            for c in self.children.values():
                c.delivery_service.send_deliveries(sender=self.id, item=ret)

            self.update_status(State.TERMINATED)
        except Exception as e:
            self.update_status(State.READY)
            raise Exception(e)
        return ret

    def clone(self):
        tmp = copy(self)
        tmp.init()
        return tmp

    def to_file(self, filename):
        '''
        # dill.dump(self, open(filename, 'wb'))
        s = self.id + '\n'
        s += self.source
        with open(filename, 'w') as f:
            f.write(s)
        '''
        with open(filename, 'w') as f:
            f.write(self.to_json())

    @classmethod
    def from_file(cls, filename):
        '''
        # return dill.load(open(filename, 'rb'))
        id = None
        source = str()
        with open(filename, 'r') as f:
            for i, l in enumerate(f.readlines()):
                if i == 0:
                    id = l.replace('\n', '')
                else:
                    source += l
        return Code(id=id, source=source)
        '''
        ret = None
        with open(filename, 'r') as f:
            ret = cls.from_json(f.read())
        return ret

    def to_dict(self):
        d = dict()
        d['_id'] = self.id
        d['source'] = self.source
        d['description'] = self.description
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['_id'], source=d['source'])
