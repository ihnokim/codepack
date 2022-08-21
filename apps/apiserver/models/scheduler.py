from pydantic import BaseModel


class Job(BaseModel):
    job_id: str = None
    trigger: str
    trigger_config: dict


class JobWithJsonArgPack(Job):
    argpack: dict = {}


class JobWithJsonCodePackAndJsonArgPack(Job):
    codepack: dict
    argpack: dict = {}


class JobWithJsonSnapshot(Job):
    snapshot: dict
