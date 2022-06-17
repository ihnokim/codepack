from pydantic import BaseModel


class Job(BaseModel):
    job_id: str = None
    trigger: str
    trigger_config: dict


class CodePackIDJob(Job):
    id: str
    argpack: dict = {}


class IDPairJob(Job):
    codepack_id: str
    argpack_id: str


class CodePackJSONJob(Job):
    codepack: dict
    argpack: dict = {}


class SnapshotJSONJob(Job):
    snapshot: dict
