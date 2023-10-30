from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class AsyncStateMixin:
    async def handle(self, context: 'Job') -> None:
        pass  # pragma: no cover
