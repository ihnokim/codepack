from pydantic import BaseModel


class CodePackID(BaseModel):
    id: str
    argpack: str = '{}'


class IDPair(BaseModel):
    codepack_id: str
    argpack_id: str


class CodePackJSON(BaseModel):
    codepack: str


class SnapshotJSON(BaseModel):
    snapshot: str
