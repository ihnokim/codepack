from pydantic import BaseModel
from typing import Optional, List


class SearchQuery(BaseModel):
    query: str
    projection: Optional[List[str]] = None


class JsonSnapshot(BaseModel):
    snapshot: dict
