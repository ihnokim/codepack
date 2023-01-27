from pydantic import BaseModel


class JsonCodePack(BaseModel):
    codepack: dict


class JsonCodePackAndJsonArgPack(BaseModel):
    codepack: dict
    argpack: dict = {}
