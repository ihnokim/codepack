import abc
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class State(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def handle(self, context: 'Job') -> None:
        pass  # pragma: no cover

    @classmethod
    @abc.abstractmethod
    def get_name(cls) -> str:
        pass  # pragma: no cover
