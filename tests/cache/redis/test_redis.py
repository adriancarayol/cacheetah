import time
import pytest
import redis

from cacheetah.cache.redis.decorators import redis_cache
from cacheetah.cache.redis.redis import RedisCacheDict
from cacheetah.compressors.cache_compressors import BZIPCacheCompressor


@pytest.fixture(scope="session")
def redis_client():
    return redis.StrictRedis()


@pytest.fixture()
def track_cache_call():
    return [0, ]


@pytest.mark.parametrize(
    "value, compressor",
    [
        (1, None),
        (2.000, None),
        ("value", None),
        ({"key": "value"}, None),
        (1, BZIPCacheCompressor()),
        (2.000, BZIPCacheCompressor()),
        ("value", BZIPCacheCompressor()),
        ({"key": "value"}, BZIPCacheCompressor()),
    ],
)
def test_set_data(redis_client, value, compressor):
    redis_cache = RedisCacheDict(redis_client, compressor=compressor)
    redis_cache["key"] = value
    assert redis_cache.get("key") == value


def test_max_size(redis_client):
    redis_cache = RedisCacheDict(redis_client, max_size=3)
    redis_cache["key1"] = "value1"
    redis_cache["key2"] = "value2"
    redis_cache["key3"] = "value3"
    redis_cache["key_4"] = "value4"
    assert redis_cache["key1"] is None


def test_expire(redis_client):
    redis_cache = RedisCacheDict(redis_client, expiration=1)
    redis_cache["key1"] = "value1"
    time.sleep(1)
    assert redis_cache["key"] is None


def test_decorator_cache(redis_client, track_cache_call):
    @redis_cache(redis_client, max_size=3, expiration=2)
    def foo():
        track_cache_call[0] += 1
        return "foo_result"

    first_call = foo()
    second_call = foo()

    assert first_call == second_call
    assert track_cache_call[0] == 1


def test_decorator_cache_expirate(redis_client, track_cache_call):
    @redis_cache(redis_client, max_size=3, expiration=1)
    def foo():
        track_cache_call[0] += 1
        return 2

    first_call = foo()
    second_call = foo()
    assert first_call == second_call
    assert track_cache_call[0] == 1
    time.sleep(1)
    third_call = foo()
    assert third_call == first_call
    assert track_cache_call[0] == 2
