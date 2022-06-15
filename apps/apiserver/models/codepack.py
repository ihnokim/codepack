from pydantic import BaseModel


class CodePackID(BaseModel):
    id: str
    argpack: dict = {}


class IDPair(BaseModel):
    codepack_id: str
    argpack_id: str


class CodePackJSON(BaseModel):
    codepack: dict
    argpack: dict = {}


class SnapshotJSON(BaseModel):
    snapshot: dict
