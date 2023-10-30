from codepack.jobs.states.state import State
from codepack.jobs.result_cache import ResultCache
from codepack.jobs.code_snapshot import CodeSnapshot
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class ReadyState(State):
    def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import States
        try:
            if not context.is_already_executed():
                code_snapshot: CodeSnapshot = context.get_code_snapshot()
                context.update_state(state=States.RUNNING)
                required_data = context.get_required_data()
                if code_snapshot.arg:
                    ret = code_snapshot.function(*code_snapshot.arg.args, **code_snapshot.arg.kwargs, **required_data)
                else:
                    ret = code_snapshot.function(**required_data)
                result = ResultCache(data=ret, serial_number=code_snapshot.serial_number, name=code_snapshot.name)
                is_saved = result.save()
                if is_saved:
                    context.update_state(state=States.TERMINATED)
                else:
                    context.update_state(state=States.ERROR)  # result cannot be saved
        except Exception as e:
            print(e)
            context.update_state(state=States.ERROR)  # TODO: add error message e

    @classmethod
    def get_name(cls) -> str:
        return 'READY'
