from codepack.storages.storable import Storable
from typing import Optional, Any, TypeVar, Iterator


Code = TypeVar('Code', bound='codepack.code.Code')


class Arg(Storable):
    def __init__(self, code: Optional[Code] = None, kwargs: Optional[dict] = None) -> None:
        Storable.__init__(self)
        self.kwargs = dict()
        if kwargs:
            for arg, value in kwargs.items():
                self.kwargs[arg] = value
        elif code:
            self.kwargs = self.extract(code)

    @staticmethod
    def extract(code: Code) -> dict:
        ret = dict()
        for param, arg in code.get_reserved_params().items():
            if param not in code.dependency.get_params().keys():
                ret[param] = arg
        return ret

    def __getitem__(self, item: str) -> Any:
        return self.kwargs[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self.kwargs[key] = value

    def __call__(self, **kwargs: Any) -> None:
        for arg, value in kwargs.items():
            self.kwargs[arg] = value

    def to_dict(self) -> dict:
        return self.kwargs

    @classmethod
    def from_dict(cls, d: dict) -> 'Arg':
        return cls(kwargs=d)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.kwargs, item)

    def __iter__(self) -> Iterator[str]:
        return self.kwargs.__iter__()

    def __str__(self) -> str:
        ret = '%s(' % self.__class__.__name__
        for i, (arg, value) in enumerate(self.kwargs.items()):
            if i:
                ret += ', '
            ret += '%s=%s' % (arg, value)
        ret += ')'
        return ret

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover
