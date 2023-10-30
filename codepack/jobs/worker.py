from codepack.jobs.job import Job
from codepack.jobs.job_manager import JobManager


class Worker(JobManager):
    def handle(self, job: Job) -> None:
        job.save()
        job.set_callback(on_transition=self.notify)
        job.handle()
