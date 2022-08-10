__lazy_imports__ = {
    'MemoryStorage': 'codepack.storages.memory_storage',
    'FileStorage': 'codepack.storages.file_storage',
    'MongoStorage': 'codepack.storages.mongo_storage',
    'S3Storage': 'codepack.storages.s3_storage',
    'MemoryMessageReceiver': 'codepack.storages.memory_message_receiver',
    'MemoryMessageSender': 'codepack.storages.memory_message_sender',
    'KafkaMessageReceiver': 'codepack.storages.kafka_message_receiver',
    'KafkaMessageSender': 'codepack.storages.kafka_message_sender',
}


__required__ = {
    'MemoryStorage', 'FileStorage', 'MongoStorage',
}


def __getattr__(name: str) -> type:
    from codepack import _import_one
    return _import_one(module_name=__name__, module_map=__lazy_imports__, name=name)
