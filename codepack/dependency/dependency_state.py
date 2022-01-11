from enum import Enum


class DependencyState(Enum):
    RESOLVED = 0
    PENDING = 1
    ERROR = 2

    def __str__(self):
        return self.name  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        elif isinstance(other, int):
            return self.name == self.__class__(other).name
        else:
            return False  # pragma: no cover
