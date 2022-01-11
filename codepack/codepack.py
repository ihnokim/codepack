from codepack.base import CodeBase, CodePackBase
from codepack import Code
from queue import Queue
from codepack.service import DefaultService
from codepack.utils.state import State
from parse import compile as parser
from ast import literal_eval
from codepack.snapshot import CodePackSnapshot


class CodePack(CodePackBase):
    def __init__(self, id, code, subscribe=None, serial_number=None, config_path=None, snapshot_service=None, storage_service=None):
        super().__init__(id=id, serial_number=serial_number)
        self.root = None
        self.roots = None
        self.service = None
        self.set_root(code)
        if isinstance(subscribe, CodeBase):
            self.subscribe = subscribe.id
        elif isinstance(subscribe, str):
            self.subscribe = subscribe
        else:
            self.subscribe = None
        self.codes = dict()
        self.init()
        self.init_service(snapshot_service=snapshot_service,
                          storage_service=storage_service,
                          config_path=config_path)

    def init(self):
        self.roots = self.get_roots()

    def init_service(self, snapshot_service=None, storage_service=None, config_path=None):
        self.service = dict()
        self.service['snapshot'] =\
            snapshot_service if snapshot_service else DefaultService.get_default_codepack_snapshot_service(config_path=config_path)
        self.service['storage'] =\
            storage_service if storage_service else DefaultService.get_default_codepack_storage_service(obj=self.__class__,
                                                                                                        config_path=config_path)

    def make_argpack(self):
        ret = dict()
        stack = list()
        for root in self.roots:
            stack.append(root)
            while len(stack):
                n = stack.pop(-1)
                if n.id not in ret:
                    ret[n.id] = dict()
                for arg, value in n.get_args().items():
                    if arg not in n.dependency.get_args().keys():
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

    def update_state(self, state, timestamp=None, argpack=None):
        if state:
            snapshot = self.to_snapshot(argpack=argpack, timestamp=timestamp)
            snapshot['state'] = state
            self.service['snapshot'].save(snapshot)

    def get_state(self):
        states = self.root.service['snapshot']\
            .load(serial_number=[code.serial_number for code in self.codes.values()], projection={'state'})
        if states:
            max_state = 0
            terminated = True
            for state in states:
                if state['state'] == 'ERROR':
                    return 'ERROR'
                elif state['state'] != 'TERMINATED':
                    terminated = False
                    max_state = max(State[state['state']].value, max_state)
            if terminated:
                return 'TERMINATED'
            else:
                return State(max_state).name
        return 'UNKNOWN'

    def save(self):
        self.service['storage'].save(item=self)

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

    def get_roots(self):
        roots = set()
        touched = set()
        q = Queue()
        for leave in self.get_leaves():
            q.put(leave)
            touched.add(leave.id)
        while not q.empty():
            n = q.get()
            if n.id not in self.codes:
                self.codes[n.id] = n
            for p in n.parents.values():
                if p.id not in touched:
                    q.put(p)
                    touched.add(p.id)
            if len(n.parents) == 0:
                roots.add(n)
        return roots

    def recursive_run(self, code, argpack):
        for p in code.parents.values():
            if p.get_state() != 'TERMINATED':
                code.update_state('WAITING')
                self.recursive_run(p, argpack)
        code.update_state('READY')
        code(**argpack[code.id])

    def sync_run(self, argpack):
        for leave in self.get_leaves():
            self.recursive_run(leave, argpack)

    def async_run(self, argpack=None):
        for id, code in self.codes.items():
            code(**argpack[id])

    def __call__(self, argpack=None, sync=True):
        self.init_code_state(state='READY', argpack=argpack)
        self.update_state('READY', argpack=argpack)
        if not argpack:
            argpack = self.make_argpack()
        if sync:
            self.sync_run(argpack=argpack)
            self.update_state('TERMINATED', argpack=argpack)
        else:
            self.async_run(argpack=argpack)
            self.update_state(self.get_state(), argpack=argpack)
        return self.get_result()

    def init_code_state(self, state, argpack=None):
        for id, code in self.codes.items():
            if argpack and id in argpack:
                code.update_state(state, kwargs=argpack[id])

    def get_result(self):
        if self.subscribe and self.codes[self.subscribe].get_state() == 'TERMINATED':
            return self.codes[self.subscribe].get_result()
        else:
            return None

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
        receive = dict()
        for i, line in enumerate(d['structure'].split('\n')):  # os.linesep
            split_idx = line.index('Code')
            hierarchy = len(line[1: split_idx-1])
            attr = p.parse(line[split_idx:])
            if attr['id'] not in codes:
                codes[attr['id']] = Code(id=attr['id'], source=d['source'][attr['id']])
            code = codes[attr['id']]
            receive[code.id] = literal_eval(attr['receive'])
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
        for id, code in codes.items():
            for arg, sender in receive[id].items():
                code.receive(arg) << codes[sender]
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

    def to_snapshot(self, *args, **kwargs):
        return CodePackSnapshot(self, *args, **kwargs)

    @classmethod
    def from_snapshot(cls, snapshot):
        d = snapshot.to_dict()
        d['_id'] = d['id']
        ret = cls.from_dict(d)
        ret.serial_number = d['serial_number']
        for id, code in ret.codes.items():
            code.update_serial_number(d['codes'][id])
        return ret
