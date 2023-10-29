import abc
from typing import Any, Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.utils.observable import Observable  # pragma: no cover


class Observer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update(self, observable: Optional['Observable'] = None, **kwargs: Any) -> None:
        pass  # pragma: no cover
