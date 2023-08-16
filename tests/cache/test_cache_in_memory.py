import pytest
from src.cache.exceptions import MaxMemorySizeCacheException
import polars as pl
from src.cache.in_memory import InMemoryCache


def test_get_empty_cache():
    cache = InMemoryCache()
    assert cache.get("key") is None


def test_set_and_get():
    cache = InMemoryCache()
    cache.set("key", 0)
    assert cache.get("key")["value"] == 0


# needs to revisit with bytes mocking
# def test_expired_cache():
#     cache = InMemoryCache()
#     cache.set("key", "value")
#     # Simulate time passing by setting the saved time to the past
#     cache.cache["key"]["at"] = time.time() - 31
#     assert cache.get("key") is None


def test_custom_condition_get():
    cache = InMemoryCache()
    assert cache.get("key", custom_condition=False) is None


# needs to revisit with bytes mocking
# def test_custom_condition_set():
#     cache = InMemoryCache()
#     cache.set(
#         "key",
#         pl.DataFrame([["ex", "ok", "nok"], [None, "ok", 4]]),
#         custom_condition=False,
#     )
#     assert cache.get("key") is None


def test_max_memory_size_exception():
    cache = InMemoryCache()
    with pytest.raises(MaxMemorySizeCacheException) as excp:
        cache.set("key", "x" * 500)
    assert str(excp.value) == "Max memory size =300 reached for key=key"


def test_pop():
    cache = InMemoryCache()
    cache.set("key", pl.DataFrame([[1, 2, 3, 4], [67, 2, 4, 5]]))
    cache.pop("key")
    assert cache.get("key") is None


def test_custom_condition_pop():
    cache = InMemoryCache()
    cache.set("key", 1112344)
    cache.pop("key", custom_condition=False)
    assert cache.get("key")["value"] == 1112344
