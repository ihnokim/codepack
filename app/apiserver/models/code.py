from pydantic import BaseModel


class CodeID(BaseModel):
    id: str
    args: tuple = ()
    kwargs: dict = {}


class CodeJSON(BaseModel):
    code: str


class SnapshotJSON(BaseModel):
    snapshot: str
