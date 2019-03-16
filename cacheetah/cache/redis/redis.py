import time
import uuid
from cacheetah.cache.generics import CacheDict
from cacheetah.cache.redis.context import pipeline
from loguru import logger


class RedisCacheDict(CacheDict):
    """Represents redis cache based in dictionary object
    
    Attributes:
        client: The cache client that will use the class to store the results.
        key_prefix: Prefix to be added to each key provided.
        max_size: Maximum size to store in the cache.
        expiration: Expiration time for a value stored in the cache.
        compressor: CacheCompressor object, to compress the cached values.
    """

    cache_prefix = "cacheetah".encode()
    cache_delimiter = ":".encode()
    key_prefix_delimiter = cache_delimiter * 2

    def __init__(
        self,
        client,
        key_prefix=None,
        max_size=512,
        expiration=15 * 60,
        eviction_range=None,
        encode_obj=None,
        decode_obj=None,
        compressor=None,
    ):
        super(RedisCacheDict, self).__init__(encode_obj, decode_obj)
        self.client = client
        self.max_size = max_size
        self.expiration = expiration
        self.compressor = compressor
        self._set_key_prefix(key_prefix)
        self._set_eviction_range(eviction_range)
        logger.info(
            "Redis cache initialized with key_prefix: {}, max_size: {}, expiration: {}, compressor: {}".format(
                key_prefix, max_size, expiration, compressor
            )
        )

    def _set_eviction_range(self, eviction_range):
        if eviction_range:
            self.eviction_range = eviction_range - 1
        else:
            self.eviction_range = int(self.max_size * 0.1)

    def _set_key_prefix(self, key_prefix):
        if key_prefix:
            key_prefix = str(key_prefix)
            key_prefix = key_prefix.encode()

            if self.key_prefix_delimiter in key_prefix:
                raise ValueError(
                    "Invalid key_prefix: key_prefix can't contains '::' characters."
                )

        else:
            key_prefix = uuid.uuid4().bytes

        self.key_prefix = (
            self.cache_prefix
            + self.cache_delimiter
            + key_prefix
            + self.key_prefix_delimiter
        )

    def _append_key_prefix_to_key(self, key):
        key = str(key)
        return self.key_prefix + key.encode()

    def __setitem__(self, key, value):
        current_size = self.client.zcard(self.key_prefix)

        if current_size >= self.max_size:
            start = 0
            end = self.eviction_range
            keys = self.client.zrange(self.key_prefix, start, end)
            with pipeline(self.client) as p:
                p.delete(*keys)
                p.zrem(self.key_prefix, *keys)

        key = self._append_key_prefix_to_key(key)
        value = self._dump_value_to_cache(value).encode()

        if self.compressor:
            value = self.compressor.compress(value)

        with pipeline(self.client) as p:
            p.setex(key, self.expiration, value)
            p.zadd(self.key_prefix, {key: time.time()})
            p.expire(self.key_prefix, self.expiration)

    def __getitem__(self, key):
        key = self._append_key_prefix_to_key(key)
        value = self.client.get(key)

        if value is None:
            return None

        if self.compressor:
            value = self.compressor.decompress(value).decode()

        value = self._load_value_from_cache(value)

        with pipeline(self.client) as p:
            p.zadd(self.key_prefix, {key: time.time()})
            p.expire(self.key_prefix, self.expiration)

        return value
