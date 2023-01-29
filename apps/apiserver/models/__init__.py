from pydantic import BaseModel
from typing import Optional, List


class JsonSnapshot(BaseModel):
    snapshot: dict
