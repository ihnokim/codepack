from typing import Any, Dict, Optional
from codepack.item import Item
from datetime import datetime
import json
from codepack.utils.compressors import Compressors
from codepack.utils.compressors.compressor import Compressor


class ResultCache(Item):
    def __init__(self, data: Optional[Any],
                 name: str,
                 serial_number: str,
                 timestamp: Optional[float] = None,
                 encoding: str = 'utf-8',
                 compression: Optional[Compressor] = Compressors.GZIP):
        self.data: Any = data
        self.timestamp = timestamp if timestamp else datetime.now().timestamp()
        self.serial_number: str = serial_number
        self.encoding = encoding
        self.compression = compression
        super().__init__(name=name,
                         version=None,
                         owner=None,
                         description=None)

    def get_id(self) -> str:
        return self.serial_number

    def __serialize__(self) -> Dict[str, Any]:
        return {'serial_number': self.serial_number,
                'name': self.name,
                'data': self.compress(data=self.data,
                                      encoding=self.encoding,
                                      compression=self.compression),
                'timestamp': self.timestamp,
                'encoding': self.encoding,
                'compression': self.compression.get_name() if self.compression else None}

    @classmethod
    def compress(cls, data: Any, encoding: str, compression: Optional[Compressor] = Compressors.GZIP) -> bytes:
        encoded = json.dumps(data).encode(encoding=encoding)
        if compression:
            return compression.compress(data=encoded)
        else:
            return encoded

    @classmethod
    def decompress(cls, data: bytes, encoding: str, compression: Optional[Compressor] = Compressors.GZIP) -> Any:
        if compression:
            return json.loads(compression.decompress(data).decode(encoding=encoding))
        else:
            return json.loads(data.decode(encoding=encoding))

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> Item:
        return cls(serial_number=d['serial_number'],
                   name=d['name'],
                   data=cls.decompress(data=d['data'],
                                       encoding=d['encoding'],
                                       compression=getattr(Compressors, d['compression'])
                                       if d['compression'] else None),
                   timestamp=d['timestamp'],
                   encoding=d['encoding'],
                   compression=d['compression'])

    def get_data(self) -> Any:
        return self.data
