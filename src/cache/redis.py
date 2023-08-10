import logging
from typing import Any
import redis
from src.cache.exceptions import MaxMemorySizeCacheException
from src.cache.base import BaseCache


_LOGGER = logging.getLogger()
_MAX_MEMORY_SIZE_PER_VALUE = 300
_TTL = 30


class RedisCache(BaseCache):
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0) -> None:
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db)

    def get(self, key: str, custom_condition: bool = True) -> Any:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        if (content := self.redis_client.get(key)) is not None:
            self._log_cache_hit(key, _LOGGER)
            return content
        else:
            self._log_cache_missed(key, _LOGGER)

        return None

    def set(self, key: str, value: Any, custom_condition: bool = True) -> None:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        if self._size_of(value) > _MAX_MEMORY_SIZE_PER_VALUE:
            raise MaxMemorySizeCacheException(
                f"Max memory size ={_MAX_MEMORY_SIZE_PER_VALUE} reached for key={key}"
            )

        value = self._build_value(value)
        self.redis_client.set(key, value, ex=_TTL)

    def pop(self, key: str, custom_condition: bool = True) -> None:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        self.redis_client.delete(key)
