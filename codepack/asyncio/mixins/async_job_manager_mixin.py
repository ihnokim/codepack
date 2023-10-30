import abc
from codepack.jobs.job import Job
from codepack.jobs.async_job import AsyncJob
from typing import Union


class AsyncJobManagerMixin:
    @abc.abstractmethod
    async def handle(self, job: Job) -> None:
        pass  # pragma: no cover

    async def start(self, interval: float = 1.0) -> None:
        await self.receiver.poll(callback=self._process_message,
                                 interval=interval,
                                 background=self.background)

    async def stop(self) -> None:
        await self.receiver.stop()

    async def notify(self, job: Job) -> None:
        await self.sender.send(topic=self.topic, message=job.to_json())

    async def _process_message(self, message: Union[str, bytes]) -> None:
        await self.handle(job=AsyncJob.from_json(message))
