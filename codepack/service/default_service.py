from codepack.service.delivery_service import DeliveryServiceAlias
from codepack.service.storage_service import StorageServiceAlias
from codepack.service.snapshot_service import SnapshotServiceAlias
from codepack.utils import Singleton
from codepack.snapshot import CodeSnapshot, CodePackSnapshot
from codepack.delivery import Delivery
from codepack.utils.config import get_default_service_config


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
            config = get_default_service_config(section='cache', config_path=config_path)
            cls.delivery_service = cls.get_delivery_service(obj=Delivery, **config)
        return cls.delivery_service

    @classmethod
    def get_default_code_storage_service(cls, obj, config_path=None):
        if not cls.code_storage_service:
            config = get_default_service_config(section='code_storage', config_path=config_path)
            cls.code_storage_service = cls.get_storage_service(obj=obj, **config)
        return cls.code_storage_service

    @classmethod
    def get_default_code_snapshot_service(cls, config_path=None):
        if not cls.code_snapshot_service:
            config = get_default_service_config(section='code_snapshot', config_path=config_path)
            cls.code_snapshot_service = cls.get_snapshot_service(obj=CodeSnapshot, **config)
        return cls.code_snapshot_service

    @classmethod
    def get_default_codepack_storage_service(cls, obj, config_path=None):
        if not cls.codepack_storage_service:
            config = get_default_service_config(section='codepack_storage', config_path=config_path)
            cls.codepack_storage_service = cls.get_storage_service(obj=obj, **config)
        return cls.codepack_storage_service

    @classmethod
    def get_default_codepack_snapshot_service(cls, config_path=None):
        if not cls.codepack_snapshot_service:
            config = get_default_service_config(section='codepack_snapshot', config_path=config_path)
            cls.codepack_snapshot_service = cls.get_snapshot_service(obj=CodePackSnapshot, **config)
        return cls.codepack_snapshot_service

    @classmethod
    def get_default_argpack_storage_service(cls, obj, config_path=None):
        if not cls.argpack_storage_service:
            config = get_default_service_config(section='argpack_storage', config_path=config_path)
            cls.argpack_storage_service = cls.get_storage_service(obj=obj, **config)
        return cls.argpack_storage_service
