from pydantic import BaseModel


class ArgPackJSON(BaseModel):
    argpack: str
