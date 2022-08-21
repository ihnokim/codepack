from pydantic import BaseModel


class JsonArgPack(BaseModel):
    argpack: dict
