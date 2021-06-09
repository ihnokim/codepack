import dill
import bson
import json
from codepack.abc import AbstractCode
from codepack.status import Status
from queue import Queue
from codepack.interface import MongoDB
from copy import deepcopy


class CodePack:
    def __init__(self, id, code, subscribe=None):
        self.id = id
        self.root = None
        self.roots = None
        self.output = None
        self.arg_cache = None
        self.set_root(code)
        if isinstance(subscribe, AbstractCode):
            self.subscribe = subscribe.id
        elif isinstance(subscribe, str):
            self.subscribe = subscribe
        else:
            self.subscribe = None
        self.codes = dict()
        self.init()

    def init(self):
        self.arg_cache = dict()
        self.output = None
        self.roots = self.get_roots(init=True)

    def set_root(self, code):
        if not isinstance(code, AbstractCode):
            raise TypeError(type(code))
        self.root = code

    def __str__(self):
        ret = 'CodePack(id: %s, subscribe: %s)\n' % (self.id, self.subscribe)
        stack = list()
        hierarchy = 0
        first_token = True
        for root in self.roots:
            stack.append((root, hierarchy))
            while len(stack):
                n, h = stack.pop(-1)
                if not first_token:
                    ret += '\n'
                else:
                    first_token = False
                ret += '|%s %s' % ('-' * h, n)
                for c in n.children.values():
                    stack.append((c, h + 1))
        return ret

    def __repr__(self):
        return self.__str__()

    def get_leaves(self):
        leaves = set()
        q = Queue()
        q.put(self.root)
        while not q.empty():
            n = q.get()
            for c in n.children.values():
                q.put(c)
            if len(n.children) == 0:
                leaves.add(n)
        return leaves

    def get_roots(self, init=False):
        roots = set()
        q = Queue()
        for leave in self.get_leaves():
            q.put(leave)
        while not q.empty():
            n = q.get()
            if init:
                n.get_ready()
                self.codes[n.id] = n
            for p in n.parents.values():
                q.put(p)
            if len(n.parents) == 0:
                roots.add(n)
        return roots

    def recursive_run(self, code, arg_dict):
        senders = code.delivery_service.get_senders().values()
        redo = False
        for p in code.parents.values():
            if p.status != Status.TERMINATED or arg_dict[p.id] != self.arg_cache[p.id]:
                if p.id in senders:
                    redo = True
                self.recursive_run(p, arg_dict)
        if code.id not in self.arg_cache or arg_dict[code.id] != self.arg_cache[code.id] or redo:
            self.arg_cache[code.id] = deepcopy(arg_dict[code.id])
            tmp = code(**arg_dict[code.id])
            if code.id == self.subscribe:
                self.output = tmp

    def __call__(self, arg_dict):
        for leave in self.get_leaves():
            self.recursive_run(leave, arg_dict)
        return self.output

    def to_file(self, filename):
        self.init() # clone
        dill.dump(self, open(filename, 'wb'))

    @staticmethod
    def from_file(filename):
        return dill.load(open(filename, 'rb'))

    def to_binary(self):
        self.init() # clone
        return bson.Binary(dill.dumps(self))

    def to_dict(self):
        d = dict()
        d['_id'] = self.id
        d['subscribe'] = self.subscribe
        d['structure'] = self.get_structure()
        d['source'] = {id: code.source for id, code in self.codes.items()}
        return d

    def to_json(self):
        return json.dumps(self.to_dict())

    def get_structure(self):
        ret = str()
        stack = list()
        hierarchy = 0
        first_token = True
        for root in self.roots:
            stack.append((root, hierarchy))
            while len(stack):
                n, h = stack.pop(-1)
                if not first_token:
                    ret += '\n'
                else:
                    first_token = False
                ret += '|%s %s' % ('-' * h, n.get_info(status=False))
                for c in n.children.values():
                    stack.append((c, h + 1))
        return ret

    @staticmethod
    def from_binary(b):
        return dill.loads(b)

    def to_db(self, db, collection, config):
        self.init()
        mc = MongoDB(config)
        mc[db][collection].insert_one(self.to_dict())
        mc.close()

    '''
    @staticmethod
    def from_db(id, db, collection, config):
        mc = MongoDB(config)
        ret = mc[db][collection].find_one({'_id': id})
        if ret is None:
            return ret
        else:
            return CodePack.from_binary(ret['binary'])
    '''