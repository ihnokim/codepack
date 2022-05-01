from codepack.code import Code
from codepack.base.codepack_base import CodePackBase
from codepack.argpack import ArgPack
from codepack.utils.config.default import Default
from codepack.plugins.state import State
from parse import compile as parser
from ast import literal_eval
from queue import Queue
from typing import Any, TypeVar, Optional, Union


CodePackSnapshot = TypeVar('CodePackSnapshot', bound='codepack.plugins.snapshots.codepack_snapshot.CodePackSnapshot')
SnapshotService = TypeVar('SnapshotService', bound='codepack.plugins.snapshot_service.SnapshotService')
StorageService = TypeVar('StorageService', bound='codepack.plugins.storage_service.StorageService')


class CodePack(CodePackBase):
    def __init__(self, id: str, code: Code, subscribe: Optional[Union[Code, str]] = None,
                 serial_number: Optional[str] = None, config_path: Optional[str] = None,
                 snapshot_service: Optional[SnapshotService] = None,
                 storage_service: Optional[StorageService] = None,
                 argpack_service: Optional[StorageService] = None,
                 owner: Optional[str] = None) -> None:
        super().__init__(id=id, serial_number=serial_number)
        self.root = None
        self.roots = None
        self.subscribe = None
        self.service = None
        self.owner = None
        self._set_root(code)
        self.codes = dict()
        self.init()
        self.init_subscription(subscribe=subscribe)
        self.init_service(snapshot_service=snapshot_service,
                          storage_service=storage_service,
                          argpack_service=argpack_service,
                          config_path=config_path)
        self._set_str_attr(key='owner', value=owner)

    def init(self) -> None:
        self.roots = self._get_roots()

    def init_service(self, snapshot_service: Optional[SnapshotService] = None,
                     storage_service: Optional[StorageService] = None,
                     argpack_service: Optional[StorageService] = None, config_path: Optional[str] = None) -> None:
        self.service = dict()
        self.service['snapshot'] =\
            snapshot_service if snapshot_service else Default.get_service('codepack_snapshot', 'snapshot_service',
                                                                          config_path=config_path)
        self.service['storage'] =\
            storage_service if storage_service else Default.get_service('codepack', 'storage_service',
                                                                        config_path=config_path)
        self.service['argpack'] = \
            argpack_service if argpack_service else Default.get_service('argpack', 'storage_service',
                                                                        config_path=config_path)

    def init_subscription(self, subscribe: Optional[Union[Code, str]] = None) -> None:
        if isinstance(subscribe, Code):
            self.subscribe = subscribe.id
        elif isinstance(subscribe, str):
            self.subscribe = subscribe
        else:
            self.subscribe = None

    def _set_str_attr(self, key: str, value: str) -> None:
        if value is None or value == 'None' or value == 'null':
            _value = None
        else:
            _value = value
        setattr(self, key, _value)

    def make_argpack(self) -> ArgPack:
        return ArgPack(self)

    def save_argpack(self, argpack: ArgPack) -> None:
        self.service['argpack'].save(item=argpack)

    def load_argpack(self, id: str) -> ArgPack:
        return self.service['argpack'].load(id=id)

    def _set_root(self, code: Code) -> None:
        if not isinstance(code, Code):
            raise TypeError(type(code))  # pragma: no cover
        self.root = code

    def __str__(self) -> str:
        ret = '%s(id: %s, subscribe: %s' % (self.__class__.__name__, self.id, self.subscribe)
        additional_item = 'owner'
        item = getattr(self, additional_item)
        if item:
            ret += ', %s: %s' % (additional_item, item)
        ret += ')\n'
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

    def __repr__(self) -> str:
        return self.__str__()

    def save_snapshot(self, timestamp: Optional[float] = None, argpack: Optional[Union[ArgPack, dict]] = None) -> None:
        snapshot = self.to_snapshot(argpack=argpack, timestamp=timestamp)
        self.service['snapshot'].save(snapshot)

    def get_state(self) -> str:
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

    def get_message(self) -> dict:
        messages = self.root.service['snapshot'] \
            .load(serial_number=[code.serial_number for code in self.codes.values()], projection={'id', 'message'})
        ret = dict()
        if messages:
            for message in messages:
                if message['message']:
                    ret[message['id']] = message['message']
        return ret

    def save(self, update: bool = False) -> None:
        self.service['storage'].save(item=self, update=update)

    @classmethod
    def load(cls, id: Union[str, list], storage_service: Optional[StorageService] = None)\
            -> Optional[Union['CodePack', list]]:
        if storage_service is None:
            storage_service = Default.get_service('codepack', 'storage_service')
        return storage_service.load(id)

    @classmethod
    def remove(cls, id: Union[str, list], storage_service: Optional[StorageService] = None) -> None:
        if storage_service is None:
            storage_service = Default.get_service('codepack', 'storage_service')
        storage_service.remove(id=id)

    def _get_leaves(self) -> set:
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

    def _get_roots(self) -> set:
        roots = set()
        touched = set()
        q = Queue()
        for leave in self._get_leaves():
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

    def _recursive_run(self, code: Code, argpack: Union[ArgPack, dict]) -> None:
        for p in code.parents.values():
            if p.get_state() != 'TERMINATED':
                code.update_state('WAITING')
                self._recursive_run(p, argpack)
        code.update_state('READY')
        code(**argpack[code.id])

    def sync_run(self, argpack: Union[ArgPack, dict]) -> None:
        for leave in self._get_leaves():
            self._recursive_run(leave, argpack)

    def async_run(self, argpack: Union[ArgPack, dict]) -> None:
        for id, code in self.codes.items():
            code(**argpack[id])

    def __call__(self, argpack: Optional[Union[ArgPack, dict]] = None, sync: bool = True) -> Any:
        self.init_code_state(state='READY', argpack=argpack)
        self.save_snapshot(argpack=argpack)
        if not argpack:
            argpack = self.make_argpack()
        if sync:
            self.sync_run(argpack=argpack)
        else:
            self.async_run(argpack=argpack)
        return self.get_result()

    def init_code_state(self, state: Union[State, str], argpack: Optional[Union[ArgPack, dict]] = None) -> None:
        for id, code in self.codes.items():
            if argpack and id in argpack:
                if isinstance(argpack[id], dict):
                    _kwargs = argpack[id]
                else:
                    _kwargs = argpack[id].to_dict()
                code.update_state(state, kwargs=_kwargs)

    def get_result(self) -> Optional[Any]:
        if self.subscribe and self.codes[self.subscribe].get_state() == 'TERMINATED':
            return self.codes[self.subscribe].get_result()
        else:
            return None

    def to_dict(self) -> dict:
        d = dict()
        d['_id'] = self.id
        d['subscribe'] = self.subscribe
        d['structure'] = self.get_structure()
        d['source'] = self.get_source()
        d['owner'] = self.owner
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'CodePack':
        root = None
        stack = list()
        codes = dict()
        receive = dict()
        for i, line in enumerate(d['structure'].split('\n')):  # os.linesep
            split_idx = line.index('Code')
            hierarchy = len(line[1: split_idx-1])
            code_str = line[split_idx:]
            p = parser(Code.blueprint(code_str))
            attr = p.parse(code_str).named
            if attr['id'] not in codes:
                codes[attr['id']] = Code(id=attr['id'], source=d['source'][attr['id']],
                                         env=attr.get('env', None), image=attr.get('image', None),
                                         owner=attr.get('owner', None))
            code = codes[attr['id']]
            receive[code.id] = literal_eval(attr['receive'])
            if i == 0:
                root = code
            while len(stack) and stack[-1][1] >= hierarchy:
                n, h = stack.pop(-1)
                if len(stack) > 0 and n.id not in stack[-1][0].children:
                    stack[-1][0] >> n
            stack.append((code, hierarchy))
        while len(stack):
            n, h = stack.pop(-1)
            if len(stack) > 0 and n.id not in stack[-1][0].children:
                stack[-1][0] >> n
        for id, code in codes.items():
            for arg, sender in receive[id].items():
                code.receive(arg) << codes[sender]
        return cls(d['_id'], code=root, subscribe=d['subscribe'], owner=d.get('owner', None))

    def get_source(self) -> dict:
        return {id: code.source for id, code in self.codes.items()}

    def get_structure(self) -> str:
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

    def to_snapshot(self, *args: Any, **kwargs: Any) -> CodePackSnapshot:
        return self.service['snapshot'].convert_to_snapshot(self, *args, **kwargs)

    @classmethod
    def from_snapshot(cls, snapshot: CodePackSnapshot) -> 'CodePack':
        d = snapshot.to_dict()
        d['_id'] = d['id']
        ret = cls.from_dict(d)
        ret.serial_number = d['serial_number']
        for id, code in ret.codes.items():
            code.update_serial_number(d['codes'][id])
        return ret
