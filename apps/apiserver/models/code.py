from pydantic import BaseModel


class Args(BaseModel):
    args: tuple = ()
    kwargs: dict = {}


class JsonCode(BaseModel):
    code: dict


class JsonCodeAndArgs(BaseModel):
    code: dict
    args: tuple = ()
    kwargs: dict = {}
