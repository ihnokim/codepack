import boto3
from botocore.config import Config
from codepack.interface import Interface
from typing import Union


class S3(Interface):
    def __init__(self, config: dict, *args, **kwargs):
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        self.session = boto3.client(config=Config(retries=dict(max_attempts=3)), *args, **self.config, **kwargs)
        self._closed = False
        return self.session

    def list_objects(self, bucket: str, prefix: str = '', *args, **kwargs):
        ret = list()
        if self.session is None:
            return ret
        for page in self.session.get_paginator('list_objects').paginate(Bucket=bucket, Prefix=prefix, *args, **kwargs):
            ret += page.get('Contents', list())
        return ret

    def download(self, bucket: str, key: str, *args, **kwargs):
        obj = self.session.get_object(Bucket=bucket, Key=key, *args, **kwargs)
        return obj['Body'].read()

    def delete(self, bucket: str, key: str, *args, **kwargs):
        return self.session.delete_object(Bucket=bucket, Key=key, *args, **kwargs)

    def upload(self, bucket: str, key: str, data: Union[str, bytes], *args, **kwargs):
        return self.session.put_object(Bucket=bucket, Key=key, Body=data, *args, **kwargs)

    def close(self):
        self._closed = True
