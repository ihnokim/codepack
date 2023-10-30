from codepack.jobs.states.state import State
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class ErrorState(State):
    def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import States
        context.propagate_down(state=States.ERROR)

    @classmethod
    def get_name(cls) -> str:
        return 'ERROR'
