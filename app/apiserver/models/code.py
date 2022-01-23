from pydantic import BaseModel
from typing import Optional


class NewCodeFromStorageToRun(BaseModel):
    id: str
    args: tuple = ()
    kwargs: dict = {}
    mode: Optional[str] = None
