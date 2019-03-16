"""
This module contains compressors for entries in cache
"""

import gzip


class CacheCompressor(object):
    """
    Base cache compressor
    """

    def __init__(self, compress_level=7):
        self.compress_level = compress_level

    def compress(self, payload):
        raise NotImplementedError("Implement a compress algorithm.")

    def decompress(self, payload):
        raise NotImplementedError("Implement a decompress algorithm.")


class BZIPCacheCompressor(CacheCompressor):
    """
    BZIP cache compressor
    """

    def compress(self, payload):
        """
        Compress the payload
        :param payload: Data in bytes
        :return: Data compressed with bzip algorithm
        """
        return gzip.compress(payload, self.compress_level)

    def decompress(self, payload):
        """
        Decompress the payload
        :param payload: Data in bytes
        :return: Data decompressed with bzip algorithm
        """
        return gzip.decompress(payload)

    def __repr__(self):
        return "<BZIPCacheCompressor compressor_level = {}>".format(self.compress_level)
