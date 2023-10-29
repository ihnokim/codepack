import gzip
from codepack.utils.compressors.compressor import Compressor


class Gzip(Compressor):
    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data=data)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data=data)

    def get_name(self) -> str:
        return 'GZIP'
