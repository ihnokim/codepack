__lazy_imports__ = {
    'MongoDB': 'codepack.interfaces.mongodb',
    'MySQL': 'codepack.interfaces.mysql',
    'MSSQL': 'codepack.interfaces.mssql',
    'DynamoDB': 'codepack.interfaces.dynamodb',
    'OracleDB': 'codepack.interfaces.oracledb',
    'Docker': 'codepack.interfaces.docker',
    'KafkaConsumer': 'codepack.interfaces.kafka_consumer',
    'KafkaProducer': 'codepack.interfaces.kafka_producer',
    'S3': 'codepack.interfaces.s3',
}


__required__ = {
    'MongoDB',
}


def __getattr__(name: str) -> type:
    from codepack import _import_one
    return _import_one(module_name=__name__, module_map=__lazy_imports__, name=name)
