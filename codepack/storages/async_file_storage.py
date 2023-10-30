import json
from typing import Dict, List, Optional, Any
from codepack.storages.file_storage import FileStorage
from codepack.interfaces.async_file_interface import AsyncFileInterface
from codepack.asyncio.mixins.async_storage_mixin import AsyncStorageMixin


class AsyncFileStorage(AsyncStorageMixin, FileStorage):
    def __init__(self, path: str) -> None:
        super().__init__(path=path)
        self.interface = AsyncFileInterface(path=path)

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return AsyncFileInterface.parse_config(config=config)

    async def save(self, id: str, item: Dict[str, Any]) -> bool:
        if not await self.exists(id=id):
            await self.interface.save_file(dirname=self.path,
                                           filename=self.get_filename(id=id),
                                           data=json.dumps(item))
            return True
        else:
            return False

    async def load(self, id: str) -> Optional[Dict[str, Any]]:
        if await self.exists(id=id):
            item_json = await self.interface.load_file(dirname=self.path,
                                                       filename=self.get_filename(id=id))
            return json.loads(item_json)
        else:
            return None

    async def update(self, id: str, **kwargs: Any) -> bool:
        if await self.exists(id=id):
            d = await self.load(id=id)
            for k, v in kwargs.items():
                d[k] = v
            await self.interface.save_file(dirname=self.path,
                                           filename=self.get_filename(id=id),
                                           data=json.dumps(d))
            return True
        else:
            return False

    async def remove(self, id: str) -> bool:
        if await self.exists(id=id):
            self.interface.remove_file(dirname=self.path,
                                       filename=self.get_filename(id=id))
            return True
        else:
            return False

    async def exists(self, id: str) -> bool:
        item_filename = self.get_filename(id=id)
        for filename in self.interface.listdir(self.path):
            if item_filename == filename:
                return True
        return False

    async def search(self, key: str, value: Any) -> List[str]:
        ret = list()
        ids = await self.list_all()
        for id in ids:
            d = await self.load(id=id)
            if d[key] == value:
                ret.append(id)
        return ret

    async def count(self, id: Optional[str] = None) -> int:
        if id:
            return len(await self.list_like(id=id))
        else:
            return len(await self.list_all())

    async def list_all(self) -> List[str]:
        filenames = self.interface.listdir(path=self.path)
        return [filename.replace('.json', '') for filename in filenames if '.json' in filename]

    async def list_like(self, id: str) -> List[str]:
        return [x for x in await self.list_all() if id in x]

    async def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if id:
            ret = list()
            for x in id:
                if await self.exists(id=x):
                    ret.append(await self.load(id=x))
            return ret
        else:
            return [await self.load(x) for x in await self.list_all()]

    async def exists_many(self, id: List[str]) -> List[bool]:
        ids = set(await self.list_all())
        ret = list()
        for i in id:
            ret.append(i in ids)
        return ret
