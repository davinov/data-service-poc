import time
from typing import Any
import pytest
from src.cache.exceptions import MaxMemorySizeCacheException
from src.cache.file_cache import FileBaseCache


class MockFileBaseCache(FileBaseCache):
    def __init__(self):
        super().__init__(cache_dir="test_cache")

    @property
    def dumper(value: Any):
        pass

    @property
    def loader(value: Any):
        pass

    def _build_value(self, value):
        return {"value": value, "at": time.time()}


def test_get_empty_cache():
    cache = MockFileBaseCache()
    assert cache.get("key1") is None


def test_set_and_get():
    cache = MockFileBaseCache()
    cache.set("key2", "value")
    cached_value = cache.get("key2")
    __import__("ipdb").set_trace()
    assert cached_value["value"] == "value"


def test_custom_condition_get():
    cache = MockFileBaseCache()
    assert cache.get("key3", custom_condition=False) is None


def test_max_memory_size_exception():
    cache = MockFileBaseCache()

    with pytest.raises(MaxMemorySizeCacheException) as excp:
        cache.set("key4", "x" * 500)

    assert str(excp.value) == ("Max memory size =300 reached for key=key")


def test_pop():
    cache = MockFileBaseCache()
    cache.set("key5", "value")
    cache.pop("key5")
    assert cache.get("key5") is None


def test_custom_condition_pop():
    cache = MockFileBaseCache()
    cache.set("key6", "value")
    cache.pop("key6", custom_condition=False)
    assert cache.get("key6")["value"] == "value"
