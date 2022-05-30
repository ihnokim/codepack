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


def __getattr__(name):
    path = __lazy_imports__.get(name)
    if not path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import operator
    without_prefix = path.split('.', 1)[-1]
    getter = operator.attrgetter(f'{without_prefix}.{name}')
    val = getter(__import__(path))
    globals()[name] = val
    return val
