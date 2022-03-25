from codepack.argpack.arg import Arg
from codepack.storage.storable import Storable


class ArgPack(Storable):
    def __init__(self, codepack=None, id: str = None, args: dict = None):
        _id = None
        if codepack:
            _id = codepack.id
        if id:
            _id = id
        Storable.__init__(self, id=_id)
        if args:
            self.args = dict()
            for id, kwargs in args.items():
                self.args[id] = Arg.from_dict(kwargs)
        elif codepack:
            self.args = self.extract(codepack)
        else:
            self.args = dict()

    @staticmethod
    def extract(codepack):
        ret = dict()
        stack = list()
        for root in codepack.roots:
            stack.append(root)
            while len(stack):
                n = stack.pop(-1)
                if n.id not in ret:
                    ret[n.id] = Arg(n)
                for c in n.children.values():
                    stack.append(c)
        return ret

    def __getitem__(self, item):
        return self.args[item]

    def __setitem__(self, key, value):
        self.args[key] = value

    def to_dict(self):
        ret = dict()
        for id, arg in self.args.items():
            ret[id] = arg.to_dict()
        ret['_id'] = self.id
        return ret

    @classmethod
    def from_dict(cls, d):
        args = dict()
        id = None
        for k, v in d.items():
            if k == '_id':
                id = v
            else:
                args[k] = v
        return cls(id=id, args=args)

    def __getattr__(self, item):
        return getattr(self.args, item)

    def __iter__(self):
        return self.args.__iter__()

    def __str__(self):
        ret = '%s(id: %s, args: {' % (self.__class__.__name__, self.id)
        for i, (id, arg) in enumerate(self.args.items()):
            if i:
                ret += ', '
            ret += '%s%s' % (id, arg.__str__().replace('Arg(', '('))
        ret += '})'
        return ret

    def __repr__(self):
        return self.__str__()  # pragma: no cover
