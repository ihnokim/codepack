from codepack.storage.storable import Storable


class Arg(Storable):
    def __init__(self, code=None, kwargs: dict = None):
        Storable.__init__(self)
        self.kwargs = dict()
        if kwargs:
            for arg, value in kwargs.items():
                self.kwargs[arg] = value
        elif code:
            self.kwargs = self.extract(code)

    @staticmethod
    def extract(code):
        ret = dict()
        for arg, value in code.get_args().items():
            if arg not in code.dependency.get_args().keys():
                ret[arg] = value
        return ret

    def __getitem__(self, item):
        return self.kwargs[item]

    def __setitem__(self, key, value):
        self.kwargs[key] = value

    def __call__(self, **kwargs):
        for arg, value in kwargs.items():
            if arg not in self.kwargs:
                raise TypeError(arg)
            self.kwargs[arg] = value

    def to_dict(self):
        return self.kwargs

    @classmethod
    def from_dict(cls, d):
        return cls(kwargs=d)

    def __getattr__(self, item):
        return getattr(self.kwargs, item)

    def __iter__(self):
        return self.kwargs.__iter__()

    def __str__(self):
        ret = '%s(' % self.__class__.__name__
        for i, (arg, value) in enumerate(self.kwargs.items()):
            if i:
                ret += ', '
            ret += '%s=%s' % (arg, value)
        ret += ')'
        return ret

    def __repr__(self):
        return self.__str__()  # pragma: no cover
