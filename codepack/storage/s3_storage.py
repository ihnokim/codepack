from codepack.interface import S3
from codepack.storage import Storage, Storable
from typing import Union
from typing import Type


class S3Storage(Storage):
    def __init__(self, item_type: Type[Storable] = None,
                 s3: Union[S3, dict] = None, bucket: str = None, *args, **kwargs):
        super().__init__(item_type=item_type)
        self.s3 = None
        self.bucket = None
        self.new_connection = None
        self.init(s3=s3, bucket=bucket, *args, **kwargs)

    def init(self, s3: Union[S3, dict] = None, bucket: str = None, *args, **kwargs):
        self.bucket = bucket
        if isinstance(s3, S3):
            self.s3 = S3
            self.new_connection = False
        elif isinstance(s3, dict):
            self.s3 = S3(s3, *args, **kwargs)
            self.new_connection = True
        else:
            raise TypeError(type(s3))

    def close(self):
        if self.new_connection:
            self.s3.close()
        self.s3 = None
