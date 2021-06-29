import boto3
from botocore.config import Config
from codepack.interface.abc import SQLInterface
from boto3.dynamodb.types import TypeDeserializer


class DynamoDB(SQLInterface):
    def __init__(self, config, **kwargs):
        super().__init__()
        self.client = None
        self.td = TypeDeserializer()
        try:
            self.client = self.connect(config=config, **kwargs)
        except Exception as e:
            print(e)
            self.client = None

    def connect(self, config, ssh_config=None, **kwargs):
        self.config = config
        return boto3.client(config=Config(retries=dict(max_attempts=3)), **self.config, **kwargs)

    def list_tables(self, name):
        ret = list()
        if self.client is None:
            return ret
        return self.client.list_tables(ExclusiveStartTableName=name)['TableNames']

    def query(self, table, q, columns=None, preprocess=None, preprocess_args=None, preprocess_kwargs=None, dummy_column='dummy'):
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
            response = self.client.query(**params)
            items = [{k: preprocess(self.td.deserialize(v), *preprocess_args, **preprocess_kwargs) for k, v in item.items()}
                     for item in response.get('Items', list()) if dummy_column not in item]
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        return items

    def select(self, table, columns=None, preprocess=None, preprocess_args=None, preprocess_kwargs=None, dummy_column='dummy', **kwargs):
        q = str()
        if len(kwargs) > 0:
            q += self.encode_sql(**kwargs)
        return self.query(table=table, q=q, columns=columns,
                          preprocess=preprocess, preprocess_args=preprocess_args, preprocess_kwargs=preprocess_kwargs,
                          dummy_column=dummy_column)

    def describe_table(self, table):
        return self.client.describe_table(TableName=table)['Table']

    @staticmethod
    def array_parser(s, sep='\x7f', dtype=str):
        if type(s) == str:
            tmp = s.split(sep)
            ret = [dtype(i) for i in tmp]
            if len(ret) == 1:
                return ret[0]
            else:
                return ret
        else:
            return s

    def close(self):
        pass

    @staticmethod
    def do_nothing(x):
        return x
