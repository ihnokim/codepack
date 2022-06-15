from pydantic import BaseModel


class CodeID(BaseModel):
    id: str
    args: tuple = ()
    kwargs: dict = {}


class CodeJSON(BaseModel):
    code: dict
    args: tuple = ()
    kwargs: dict = {}


class SnapshotJSON(BaseModel):
    snapshot: dict
