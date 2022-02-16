from pydantic import BaseModel


class Job(BaseModel):
    job_id: str = None
    trigger: str
    trigger_config = dict


class CodePackIDJob(Job):
    id: str
    argpack: str = '{}'


class IDPairJob(Job):
    codepack_id: str
    argpack_id: str


class CodePackJSONJob(Job):
    codepack: str
    argpack: str = '{}'


class SnapshotJSONJob(Job):
    snapshot: str
