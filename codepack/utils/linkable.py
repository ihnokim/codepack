import abc
from typing import Union, Iterable, Any


class Linkable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __shallow_link__(self, other: 'Linkable') -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def __deep_link__(self, other: 'Linkable') -> Any:
        pass  # pragma: no cover

    @abc.abstractmethod
    def __unlink__(self, other: 'Linkable') -> None:
        pass  # pragma: no cover

    def __gt__(self, other: Union['Linkable', Iterable]) -> Union['Linkable', Iterable]:
        if isinstance(other, self.__class__):
            self.__shallow_link__(other)
            return other
        elif isinstance(other, Iterable):
            ret = list()
            for t in other:
                self.__gt__(t)
                ret.append(t)
            return ret
        else:
            raise TypeError(type(other))  # pragma: no cover

    def __lt__(self, other: Union['Linkable', Iterable]) -> Union['Linkable', Iterable]:
        if isinstance(other, self.__class__):
            other.__shallow_link__(self)
            return other
        elif isinstance(other, Iterable):
            ret = list()
            for t in other:
                self.__lt__(t)
                ret.append(t)
            return ret
        else:
            raise TypeError(type(other))  # pragma: no cover

    def __rshift__(self, other: Union['Linkable', Iterable]) -> Union[Any, Iterable]:
        if isinstance(other, self.__class__):
            return self.__deep_link__(other)
        elif isinstance(other, Iterable):
            ret = list()
            for t in other:
                ret.append(self.__rshift__(t))
            return ret
        else:
            raise TypeError(type(other))  # pragma: no cover

    def __lshift__(self, other: Union['Linkable', Iterable]) -> Union[Any, Iterable]:
        if isinstance(other, self.__class__):
            return other.__deep_link__(self)
        elif isinstance(other, Iterable):
            ret = list()
            for t in other:
                ret.append(self.__lshift__(t))
            return ret
        else:
            raise TypeError(type(other))  # pragma: no cover

    def __truediv__(self, other: Union['Linkable', Iterable]) -> Union['Linkable', Iterable]:
        if isinstance(other, self.__class__):
            self.__unlink__(other)
            return other
        elif isinstance(other, Iterable):
            ret = list()
            for t in other:
                ret.append(self.__truediv__(t))
            return ret
        else:
            raise TypeError(type(other))  # pragma: no cover

    def __floordiv__(self, other: Union['Linkable', Iterable]) -> Union['Linkable', Iterable]:
        return self.__truediv__(other=other)
