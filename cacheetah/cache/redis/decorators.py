from functools import wraps

from cacheetah.cache.redis.redis import RedisCacheDict

CACHE_DELIMITER = ":".encode()
MODULE_DELIMITER = ".".encode()


class _HashedSeq(list):
    """ This class guarantees that hash() will be called no more than once
        per element.  This is important because the lru_cache() will hash
        the key multiple times on a cache miss.

        See functools.lru_cache from builtin python
    """

    __slots__ = "hashvalue"

    def __init__(self, tup, hash=hash):
        self[:] = tup
        self.hashvalue = hash(tup)

    def __hash__(self):
        return self.hashvalue


def _make_key(
    args,
    kwds,
    typed,
    kwd_mark=(object(),),
    fasttypes={int, str},
    tuple=tuple,
    type=type,
    len=len,
):
    """Make a cache key from optionally typed positional and keyword arguments
    The key is constructed in a way that is flat as possible rather than
    as a nested structure that would take more memory.
    If there is only a single argument and its data type is known to cache
    its hash value, then that argument is returned without a wrapper.  This
    saves space and improves lookup speed.
    See functools.lru_cache from builtin python
    """
    # All of code below relies on kwds preserving the order input by the user.
    # Formerly, we sorted() the kwds before looping.  The new way is *much*
    # faster; however, it means that f(x=1, y=2) will now be treated as a
    # distinct call from f(y=2, x=1) which will be cached separately.

    key = args
    if kwds:
        key += kwd_mark
        for item in kwds.items():
            key += item
    if typed:
        key += tuple(type(v) for v in args)
        if kwds:
            key += tuple(type(v) for v in kwds.values())
    elif len(key) == 1 and type(key[0]) in fasttypes:
        return key[0]
    return _HashedSeq(key).hashvalue


def redis_cache(
    client,
    max_size=512,
    expiration=15 * 60,
    eviction_range=None,
    encode_obj=None,
    decode_obj=None,
):
    make_key = _make_key

    def wrapper(func):
        key_prefix = CACHE_DELIMITER.join(
            x.encode().replace(MODULE_DELIMITER, CACHE_DELIMITER)
            for x in (func.__module__, func.__qualname__)
        )

        lru_cache = RedisCacheDict(
            client,
            max_size=max_size,
            key_prefix=key_prefix,
            expiration=expiration,
            eviction_range=eviction_range,
            encode_obj=encode_obj,
            decode_obj=decode_obj,
        )

        @wraps(func)
        def inner(*args, **kwargs):
            key = str(make_key(args, kwargs, False)).encode()
            value = lru_cache[key]

            if value is None:
                value = func(*args, **kwargs)
                lru_cache[key] = value
                return value

            return value

        return inner

    return wrapper
