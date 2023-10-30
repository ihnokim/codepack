from codepack.item import Item


class AsyncItemMixin:
    async def save(self) -> bool:
        storage = self.__class__.get_storage()
        return await storage.save(id=self.get_id(), item=self.to_dict())

    @classmethod
    async def load(cls, id: str) -> Item:
        storage = cls.get_storage()
        d = await storage.load(id=id)
        return cls.from_dict(d)
