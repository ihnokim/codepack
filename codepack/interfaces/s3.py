from codepack.interfaces.interface import Interface
import boto3
from botocore.config import Config
from botocore.client import BaseClient
from botocore.response import StreamingBody
from typing import Any, Union, Optional


class S3(Interface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> BaseClient:
        if 'config' not in self.config and 'config' not in kwargs:
            self.config['config'] = Config(retries=dict(max_attempts=3))
        self.session = boto3.client(*args, **self.config, **kwargs)
        self._closed = False
        return self.session

    def list_objects(self, bucket: str, prefix: str = '', *args: Any, **kwargs: Any) -> list:
        ret = list()
        if self.session is None:
            return ret
        for page in self.session.get_paginator('list_objects').paginate(Bucket=bucket, Prefix=prefix, *args, **kwargs):
            ret += page.get('Contents', list())
        return ret

    def download(self, bucket: str, key: str,
                 streaming: bool = False, *args: Any, **kwargs: Any) -> Optional[Union[bytes, StreamingBody]]:
        try:
            obj = self.session.get_object(Bucket=bucket, Key=key, *args, **kwargs)
        except self.session.exceptions.NoSuchKey:
            return None
        body = obj.get('Body', None)
        if streaming:
            return body
        elif body:
            return body.read()
        else:
            return None

    def exist(self, bucket: str, key: str, *args: Any, **kwargs: Any) -> bool:
        try:
            obj = self.session.head_object(Bucket=bucket, Key=key, *args, **kwargs)
            return obj is not None
        except self.session.exceptions.ClientError:
            return False

    def delete(self, bucket: str, key: str, *args: Any, **kwargs: Any) -> dict:
        return self.session.delete_object(Bucket=bucket, Key=key, *args, **kwargs)

    def upload(self, bucket: str, key: str, data: Union[str, bytes], *args: Any, **kwargs: Any) -> dict:
        return self.session.put_object(Bucket=bucket, Key=key, Body=data, *args, **kwargs)

    def download_file(self, bucket: str, key: str, path: str, *args: Any, **kwargs: Any) -> None:
        self.session.download_file(Bucket=bucket, Key=key, Filename=path, *args, **kwargs)

    def upload_file(self, path: str, bucket: str, key: str, *args: Any, **kwargs: Any) -> None:
        self.session.upload_file(Filename=path, Bucket=bucket, Key=key, *args, **kwargs)

    def close(self) -> None:
        self._closed = True
