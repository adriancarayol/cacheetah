import json


class CacheDict(object):
    """Represents class to implement a dictionary cache
    
    Attributes:
        encode_obj: Encoding function, for complex objects.
        decode_obj: Decoding function, for complex objects.
    """

    def __init__(self, encode_obj=None, decode_obj=None):
        self.encode_obj = encode_obj
        self.decode_obj = decode_obj

    def _dump_value_to_cache(self, value):
        return json.dumps(value, default=self.encode_obj, sort_keys=True)

    def _load_value_from_cache(self, value):
        return json.loads(value, object_hook=self.decode_obj)

    def get(self, key, default=None):
        """Returns the value stored in the cache
        according to the key provided
        
        Args:
            key: Key to access the value stored in cache
            default: If the key does not exist, return a default value
        
        Returns:
            The result stored in the cache.
            For example, if the key "my_result" is associated with the value 2 in the cache,
            it will return 2.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        raise NotImplementedError("You need to implement this function.")

    def __getitem__(self, key):
        raise NotImplementedError("You need to implement this function.")
