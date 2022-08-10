from codepack.interfaces.interface import Interface
import pymongo
from typing import Any, Optional
import bson


class MongoDB(Interface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> pymongo.mongo_client.MongoClient:
        _config = {k: v for k, v in self.config.items()}
        for k, v in kwargs.items():
            _config[k] = v
        if 'connect' in _config:
            _config['connect'] = self.eval_bool(_config['connect'])
        if 'port' in _config:
            _config['port'] = int(_config['port'])
        if self.ssh_config:
            self.inspect_config_for_sshtunnel(config=_config, host_key='host', port_key='port')
            _host, _port = self.set_sshtunnel(host=_config['host'], port=int(_config['port']))
            _config['host'] = _host
            _config['port'] = _port
        self.session = pymongo.MongoClient(*args, **_config)
        self._closed = False
        return self.session

    def __getitem__(self, item: str) -> Any:
        assert not self.closed(), "connection is closed"
        return self.session[item]

    def __getattr__(self, item: str) -> Any:
        assert not self.closed(), "connection is closed"
        return self.__getitem__(item)

    def close(self) -> None:
        self.close_sshtunnel()
        if not self.closed():
            self.session.close()
            self._closed = True

    @classmethod
    def get_objectid(cls, oid: Optional[str] = None) -> bson.objectid.ObjectId:
        return bson.objectid.ObjectId(oid=oid)
