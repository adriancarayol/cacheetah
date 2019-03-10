from cacheetah.cache.generics import CacheDict
from cacheetah.cache.redis.context import pipeline


class RedisCacheDict(CacheDict):
    """Represents redis cache based in dictionary object
    
    Attributes:
        client: The cache client that will use the class to store the results.
        key_prefix: Prefix to be added to each key provided.
        max_size: Maximum size to store in the cache.
        expiration: Expiration time for a value stored in the cache.
    """
    
    def __init__(self, client, key_prefix=None, max_size=512, expiration=15*60, encode_obj=None, decode_obj=None):
        super(RedisCacheDict, self).__init__(encode_obj, decode_obj)
        self.client = client
        self.key_prefix = key_prefix
        self.max_size = max_size
        self.expiration = expiration        
        

    def __setitem__(self, key, value):
        value = self._deserialize_value_from_cache(value)

        with pipeline(self.client) as p:
            p.setex(key, self.expiration, value)

    def __getitem__(self, key):
        value = self.client.get(key)
    
        if value is None:
            return None
        
        value = self._serialize_value_to_cache(value)
        return value
            