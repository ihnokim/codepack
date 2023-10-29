import abc


class Compressor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def compress(self, data: bytes) -> bytes:
        pass  # pragma: no cover

    @abc.abstractmethod
    def decompress(self, data: bytes) -> bytes:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_name(self) -> str:
        pass  # pragma: no cover
