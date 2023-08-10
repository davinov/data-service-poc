import logging
from typing import Any
from src.cache.exceptions import MaxMemorySizeCacheException
from src.cache.base import BaseCache
import time
import pickle

_LOGGER = logging.getLogger()
_MAX_MEMORY_SIZE_PER_VALUE = 300
_TTL = 30


class InMemoryCache(BaseCache):
    def __init__(self, cache: dict[str, Any] = {}) -> None:
        # We can set a cache from an existing dict
        self.cache: dict[str, Any] = cache

    def get(
        self, key: str, default: Any | None = None, custom_condition: bool = True
    ) -> Any:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        if (saved_value := self.cache.get(key, default)) is not None:
            saved_value = pickle.loads(saved_value)
            if time.time() - saved_value["at"] < _TTL:
                self._log_cache_hit(key, _LOGGER)
                return saved_value
            else:
                self.cache.pop(key)

        self._log_cache_missed(key, _LOGGER)
        return None

    def set(
        self, key: str, value: Any | None = None, custom_condition: bool = True
    ) -> None:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        if self._size_of(value) > _MAX_MEMORY_SIZE_PER_VALUE:
            raise MaxMemorySizeCacheException(
                f"Max memory size ={_MAX_MEMORY_SIZE_PER_VALUE} reached for key={key}"
            )

        self.cache[key] = self._build_value(value)

    def pop(self, key: str, custom_condition: bool = True) -> None:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        try:
            self.cache.pop(key)
        except KeyError:
            # ignore if the key doesn't exist
            pass
