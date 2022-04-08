from codepack.interfaces.sql_interface import SQLInterface
import boto3
from botocore.config import Config
from botocore.client import BaseClient
from boto3.dynamodb.types import TypeDeserializer
from typing import Optional, Callable, Any


class DynamoDB(SQLInterface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.td = TypeDeserializer()
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> BaseClient:
        if 'config' not in self.config and 'config' not in kwargs:
            self.config['config'] = Config(retries=dict(max_attempts=3))
        self.session = boto3.client(*args, **self.config, **kwargs)
        self._closed = False
        return self.session

    def list_tables(self, name: str) -> list:
        ret = list()
        if self.session is None:
            return ret
        return self.session.list_tables(ExclusiveStartTableName=name)['TableNames']

    def query(self, table: str, q: str, columns: Optional[list] = None, preprocess: Optional[Callable] = None,
              preprocess_args: Optional[tuple] = None, preprocess_kwargs: Optional[dict] = None,
              dummy_column: str = 'dummy') -> list:
        if preprocess is None:
            preprocess = self.do_nothing
        if preprocess_args is None:
            preprocess_args = tuple()
        if preprocess_kwargs is None:
            preprocess_kwargs = dict()
        params = {'TableName': table, 'KeyConditionExpression': q}
        if columns is not None:
            params['ProjectionExpression'] = ','.join(columns)
        done = False
        start_key = None
        items = list()
        while not done:
            if start_key:
                params['ExclusiveStartKey'] = start_key
            response = self.session.query(**params)
            items = [{k: preprocess(self.td.deserialize(v), *preprocess_args, **preprocess_kwargs) for k, v in item.items()}
                     for item in response.get('Items', list()) if dummy_column not in item]
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        return items

    def select(self, table: str, columns: Optional[list] = None, preprocess: Optional[Callable] = None,
               preprocess_args: Optional[tuple] = None, preprocess_kwargs: Optional[dict] = None,
               dummy_column: str = 'dummy', **kwargs: Any) -> list:
        q = str()
        if len(kwargs) > 0:
            q += self.encode_sql(**kwargs)
        return self.query(table=table, q=q, columns=columns,
                          preprocess=preprocess, preprocess_args=preprocess_args, preprocess_kwargs=preprocess_kwargs,
                          dummy_column=dummy_column)

    def describe_table(self, table: str) -> dict:
        return self.session.describe_table(TableName=table)['Table']

    @staticmethod
    def array_parser(s: Any, sep: str = '\x7f', dtype: type = str) -> Any:
        if type(s) == str:
            tmp = s.split(sep)
            ret = [dtype(i) for i in tmp]
            if len(ret) == 1:
                return ret[0]
            else:
                return ret
        else:
            return s

    def close(self) -> None:
        self._closed = True

    @staticmethod
    def do_nothing(x: Any, *args: Any, **kwargs: Any) -> Any:
        return x
