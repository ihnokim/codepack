from codepack.service.delivery_service import DeliveryServiceAlias
from codepack.service.storage_service import StorageServiceAlias
from codepack.service.snapshot_service import SnapshotServiceAlias
from codepack.utils import Singleton
from codepack.snapshot import CodeSnapshot, CodePackSnapshot
from codepack.delivery import Delivery
from codepack.config import Config
from importlib import import_module


class DefaultService(Singleton):
    delivery_service = None
    code_storage_service = None
    code_snapshot_service = None
    codepack_storage_service = None
    codepack_snapshot_service = None
    argpack_storage_service = None

    @classmethod
    def init(cls):
        cls.delivery_service = None
        cls.code_storage_service = None
        cls.code_snapshot_service = None
        cls.codepack_storage_service = None
        cls.codepack_snapshot_service = None
        cls.argpack_storage_service = None

    @staticmethod
    def get_delivery_service(source, *args, **kwargs):
        cls = DeliveryServiceAlias[source].value
        return cls(*args, **kwargs)

    @staticmethod
    def get_snapshot_service(source, *args, **kwargs):
        cls = SnapshotServiceAlias[source].value
        return cls(*args, **kwargs)

    @staticmethod
    def get_storage_service(source, *args, **kwargs):
        cls = StorageServiceAlias[source].value
        return cls(*args, **kwargs)

    @classmethod
    def get_default_delivery_service(cls, config_path=None):
        if not cls.delivery_service:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='cache')
            cls.delivery_service = cls.get_delivery_service(item_type=Delivery, **storage_config)
        return cls.delivery_service

    @classmethod
    def get_default_code_storage_service(cls, config_path=None):
        if not cls.code_storage_service:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='code_storage')
            cls.code_storage_service = cls.get_storage_service(
                item_type=getattr(import_module('codepack'), 'Code'), **storage_config)
        return cls.code_storage_service

    @classmethod
    def get_default_code_snapshot_service(cls, config_path=None):
        if not cls.code_snapshot_service:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='code_snapshot')
            cls.code_snapshot_service = cls.get_snapshot_service(item_type=CodeSnapshot, **storage_config)
        return cls.code_snapshot_service

    @classmethod
    def get_default_codepack_storage_service(cls, config_path=None):
        if not cls.codepack_storage_service:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='codepack_storage')
            cls.codepack_storage_service = cls.get_storage_service(
                item_type=getattr(import_module('codepack'), 'CodePack'), **storage_config)
        return cls.codepack_storage_service

    @classmethod
    def get_default_codepack_snapshot_service(cls, config_path=None):
        if not cls.codepack_snapshot_service:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='codepack_snapshot')
            cls.codepack_snapshot_service = cls.get_snapshot_service(item_type=CodePackSnapshot, **storage_config)
        return cls.codepack_snapshot_service

    @classmethod
    def get_default_argpack_storage_service(cls, config_path=None):
        if not cls.argpack_storage_service:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='argpack_storage')
            cls.argpack_storage_service = cls.get_storage_service(
                item_type=getattr(import_module('codepack.argpack'), 'ArgPack'), **storage_config)
        return cls.argpack_storage_service
