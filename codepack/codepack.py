from codepack.abc import CodeBase, CodePackBase
from codepack import Code
from queue import Queue
from copy import deepcopy
from parse import compile as parser
from ast import literal_eval


class CodePack(CodePackBase):
    def __init__(self, id, code, subscribe=None):
        super().__init__(id=id)
        self.root = None
        self.roots = None
        self.arg_cache = None
        self.set_root(code)
        if isinstance(subscribe, CodeBase):
            self.subscribe = subscribe.id
        elif isinstance(subscribe, str):
            self.subscribe = subscribe
        else:
            self.subscribe = None
        self.codes = dict()
        self.init(arg_dict=None, lazy=False)

    def init(self, arg_dict=None, lazy=False):
        self.init_arg_cache(arg_dict, lazy=lazy)
        if not lazy:
            self.roots = self.get_roots(init=True)

    def init_arg_cache(self, arg_dict, lazy=False):
        if arg_dict and lazy:
            q = Queue()
            for id in self.arg_cache:
                if self.arg_cache[id] != arg_dict[id]:
                    q.put(id)
            while not q.empty():
                id = q.get()
                self.arg_cache.pop(id, None)
                for c in self.codes[id].children.values():
                    q.put(c.id)
        else:
            self.arg_cache = dict()

    def make_arg_dict(self):
        ret = dict()
        stack = list()
        for root in self.roots:
            stack.append(root)
            while len(stack):
                n = stack.pop(-1)
                if n.id not in ret:
                    ret[n.id] = dict()
                for arg, value in n.get_args().items():
                    if arg not in n.get_dependent_args().keys():
                        ret[n.id][arg] = value
                for c in n.children.values():
                    stack.append(c)
        return ret

    def set_root(self, code):
        if not isinstance(code, CodeBase):
            raise TypeError(type(code))
        self.root = code

    def __str__(self):
        ret = '%s(id: %s, subscribe: %s)\n' % (self.__class__.__name__, self.id, self.subscribe)
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
        touched = set()
        q = Queue()
        for leave in self.get_leaves():
            q.put(leave)
            touched.add(leave.id)
        while not q.empty():
            n = q.get()
            if init:
                if n.id not in self.codes:
                    self.codes[n.id] = n
            for p in n.parents.values():
                if p.id not in touched:
                    q.put(p)
                    touched.add(p.id)
            if len(n.parents) == 0:
                roots.add(n)
        return roots

    def recursive_run(self, code, arg_dict):
        state = code.get_state()
        senders = code.get_dependent_args().values()
        redo = True if state != 'TERMINATED' else False
        for p in code.parents.values():
            if p.get_state() != 'TERMINATED' or \
                    p.id not in self.arg_cache or \
                    arg_dict[p.id] != self.arg_cache[p.id]:
                redo = True if p.id in senders else False
                code.update_state('WAITING')
                self.recursive_run(p, arg_dict)
        if redo or \
                code.id not in self.arg_cache or \
                arg_dict[code.id] != self.arg_cache[code.id]:
            self.arg_cache[code.id] = deepcopy(arg_dict[code.id])
            code.update_state('READY')
            code(**arg_dict[code.id])

    def __call__(self, arg_dict=None, lazy=False):
        ret = None
        if not arg_dict:
            arg_dict = self.make_arg_dict()
        self.init(arg_dict=arg_dict if lazy else None, lazy=lazy)
        for leave in self.get_leaves():
            self.recursive_run(leave, arg_dict)
        if self.subscribe:
            c = self.codes[self.subscribe]
            ret = c.get_result(serial_number=c.serial_number)
        return ret

    def to_dict(self):
        d = dict()
        d['_id'] = self.id
        d['subscribe'] = self.subscribe
        d['structure'] = self.get_structure()
        d['source'] = self.get_source()
        return d

    @classmethod
    def from_dict(cls, d):
        p = parser('Code(id: {id}, function: {function}, args: {args}, receive: {receive})')
        root = None
        stack = list()
        codes = dict()

        for i, line in enumerate(d['structure'].split('\n')): # os.linesep
            split_idx = line.index('Code')
            hierarchy = len(line[1: split_idx-1])
            attr = p.parse(line[split_idx:])

            if attr['id'] not in codes:
                codes[attr['id']] = Code(id=attr['id'], source=d['source'][attr['id']])

            code = codes[attr['id']]
            receive = literal_eval(attr['receive'])
            for arg, sender in receive.items():
                code.receive(arg) << sender
            if i == 0:
                root = code

            while len(stack) and stack[-1][1] >= hierarchy:
                n, h = stack.pop(-1)
                if len(stack) > 0:
                    stack[-1][0] >> n
            stack.append((code, hierarchy))

        while len(stack):
            n, h = stack.pop(-1)
            if len(stack) > 0:
                stack[-1][0] >> n
        return cls(d['_id'], code=root, subscribe=d['subscribe'])

    def get_source(self):
        return {id: code.source for id, code in self.codes.items()}

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
                ret += '|%s %s' % ('-' * h, n.get_info(state=False))
                for c in n.children.values():
                    stack.append((c, h + 1))
        return ret
