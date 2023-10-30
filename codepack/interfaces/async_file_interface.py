from codepack.interfaces.file_interface import FileInterface
import os
import aiofiles
from typing import List


class AsyncFileInterface(FileInterface):
    @classmethod
    async def save_file(cls, dirname: str, filename: str, data: str) -> None:
        path = os.path.join(dirname, filename)
        await cls.write(path=path, data=data)

    @classmethod
    async def load_file(cls, dirname: str, filename: str) -> str:
        path = os.path.join(dirname, filename)
        return await cls.read(path=path)

    @classmethod
    async def write(cls, path: str, data: str, mode: str = 'w+') -> None:
        async with aiofiles.open(path, mode) as f:
            await f.write(data)

    @classmethod
    async def read(cls, path: str) -> str:
        async with aiofiles.open(path, 'r') as f:
            data = await f.read()
        return data

    @classmethod
    async def readlines(cls, path: str) -> List[str]:
        async with aiofiles.open(path, 'r') as f:
            lines = await f.readlines()
        return lines
