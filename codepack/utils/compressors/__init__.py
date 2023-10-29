from codepack.utils.compressors.gzip import Gzip


_compressors = [Gzip]


class Compressors:
    pass


for compressor_type in _compressors:
    compressor = compressor_type()
    setattr(Compressors, compressor.get_name(), compressor)
