__lazy_imports__ = {
    'MemoryStorage': 'codepack.storages.memory_storage',
    'FileStorage': 'codepack.storages.file_storage',
    'MongoStorage': 'codepack.storages.mongo_storage',
    'S3Storage': 'codepack.storages.s3_storage',
    'MemoryMessenger': 'codepack.storages.memory_messenger',
    'KafkaMessenger': 'codepack.storages.kafka_messenger',
}


__required__ = {
    'MemoryStorage', 'FileStorage', 'MongoStorage',
}


def __getattr__(name: str) -> type:
    from codepack import _import_one
    return _import_one(module_name=__name__, module_map=__lazy_imports__, name=name)
