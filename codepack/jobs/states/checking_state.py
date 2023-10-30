from codepack.jobs.states.state import State
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class CheckingState(State):
    def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import States
        upstream_states = context.get_upstream_states()
        upstream_state = States.TERMINATED
        if upstream_states:
            for state in upstream_states:
                if state == States.TERMINATED:
                    continue
                elif state == States.ERROR:
                    upstream_state = States.ERROR
                    break
                else:
                    upstream_state = States.WAITING
                    break
            if upstream_state != States.TERMINATED:
                context.update_state(state=upstream_state)
                return
            dependency_resolved = context.check_if_dependencies_resolved()
            for snapshot, resolved in dependency_resolved.items():
                if not resolved:
                    context.update_state(state=States.ERROR)
                    return
        context.update_state(state=States.READY)

    @classmethod
    def get_name(cls) -> str:
        return 'CHECKING'
