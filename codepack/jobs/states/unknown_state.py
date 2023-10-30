from codepack.jobs.states.state import State
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class UnknownState(State):
    def handle(self, context: 'Job') -> None:
        pass  # pragma: no cover

    @classmethod
    def get_name(cls) -> str:
        return 'UNKNOWN'
