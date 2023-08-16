import pytest
from src.cache.exceptions import MaxMemorySizeCacheException
import pickle

from src.cache.redis import RedisCache


@pytest.fixture(scope="module")
def redis_cache():
    # Assuming you have Redis running on the default localhost and port
    # docker run -d -p 6379:6379 --rm --name redis redis
    return RedisCache()


def test_get_empty_cache(redis_cache):
    assert redis_cache.get("key") is None


def test_set_and_get(redis_cache):
    redis_cache.set("key", "value")
    assert pickle.loads(redis_cache.get("key"))["value"] == "value"


def test_max_memory_size_exception(redis_cache):
    with pytest.raises(MaxMemorySizeCacheException) as excp:
        redis_cache.set("key", "x" * 500)
    assert str(excp.value) == "Max memory size =300 reached for key=key"


def test_pop(redis_cache):
    redis_cache.set("key", "value")
    redis_cache.pop("key")
    assert redis_cache.get("key") is None
