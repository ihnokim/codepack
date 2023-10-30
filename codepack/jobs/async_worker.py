from codepack.jobs.job import Job
from codepack.asyncio.mixins.async_job_manager_mixin import AsyncJobManagerMixin
from codepack.jobs.worker import Worker


class AsyncWorker(AsyncJobManagerMixin, Worker):
    async def handle(self, job: Job) -> None:
        await job.save()
        job.set_callback(on_transition=self.notify)
        await job.handle()
