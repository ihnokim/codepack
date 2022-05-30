__lazy_imports__ = {
    'MemoryStorage': 'codepack.storages.memory_storage',
    'FileStorage': 'codepack.storages.file_storage',
    'MongoStorage': 'codepack.storages.mongo_storage',
    'S3Storage': 'codepack.storages.s3_storage',
    'MemoryMessenger': 'codepack.storages.memory_messenger',
    'KafkaMessenger': 'codepack.storages.kafka_messenger',
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
