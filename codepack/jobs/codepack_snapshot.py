from typing import List, Optional, Dict, Any, Iterator
from datetime import datetime
import uuid
from codepack.item import Item
from codepack.utils.double_key_map import DoubleKeyMap


class CodePackSnapshot(Item):
    def __init__(self,
                 code_snapshots: List[str],
                 links: Optional[DoubleKeyMap] = None,
                 serial_number: Optional[str] = None,
                 timestamp: Optional[float] = None,
                 name: Optional[str] = None,
                 subscription: Optional[str] = None) -> None:
        self.serial_number: str = serial_number if serial_number else str(uuid.uuid4())
        self.code_snapshots: List[str] = code_snapshots
        self.subscription: Optional[str] = subscription
        self.links: DoubleKeyMap = links if links else DoubleKeyMap()
        if len(code_snapshots) == 0:
            raise ValueError('At least one code_snapshot is required')
        self.timestamp = timestamp if timestamp else datetime.now().timestamp()
        super().__init__(name=name,
                         version=None,
                         owner=None,
                         description=None)

    def get_id(self) -> str:
        return self.serial_number

    def __serialize__(self) -> Dict[str, Any]:
        return {'serial_number': self.serial_number,
                'name': self.name,
                'code_snapshots': self.code_snapshots,
                'links': self.links.to_dict(),
                'timestamp': self.timestamp,
                'subscription': self.subscription}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> Item:
        dkm = DoubleKeyMap(map=d['links'])
        return cls(serial_number=d['serial_number'],
                   name=d['name'],
                   code_snapshots=d['code_snapshots'],
                   links=dkm,
                   timestamp=d['timestamp'],
                   subscription=d['subscription'])

    def get_downstream(self, key: str) -> List[str]:
        downstream = list()
        for src, dst in self.links.keys():
            if src == key:
                downstream.append(dst)
        return downstream

    def get_upstream(self, key: str) -> List[str]:
        upstream = list()
        for src, dst in self.links.keys():
            if dst == key:
                upstream.append(src)
        return upstream

    def get_dependencies(self, key: str) -> Dict[str, str]:
        dependencies = dict()
        for src, dst, param in self.links.items():
            if dst == key and param is not None:
                dependencies[param] = src
        return dependencies

    def __iter__(self) -> Iterator[str]:
        return self.code_snapshots.__iter__()
