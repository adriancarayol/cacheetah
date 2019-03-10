import pytest
from unittest.mock import MagicMock
from cacheetah.cache.generics import CacheDict

@pytest.fixture()
def cache_dict():
    return CacheDict()

def test_create_cache_dict(cache_dict):
    assert cache_dict.encode_obj is None
    assert cache_dict.decode_obj is None


def test_set_item(cache_dict):
    with pytest.raises(NotImplementedError):
        cache_dict['key'] = 'foo_value'

def test_get(cache_dict):
    with pytest.raises(NotImplementedError):
        cache_dict.get('foo')

def test_default_value(cache_dict, monkeypatch):
    monkeypatch.setattr('cacheetah.cache.generics.CacheDict.__getitem__', lambda key, default: 'value')
    value = cache_dict.get('foo', 'value')
    assert value == 'value'
