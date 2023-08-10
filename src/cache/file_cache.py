import os
import hashlib
import polars as pl
from typing import Any
import logging
from cache.base import BaseCache
import time

from cache.exceptions import MaxMemorySizeCacheException

_LOGGER = logging.getLogger()
_MAX_MEMORY_SIZE_PER_VALUE = 300
_TTL = 30


class FileBaseCache(BaseCache):
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _build_key(self, key: str) -> str:
        return hashlib.md5(key.encode("utf-8")).hexdigest()

    def _get_cache_filename(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{self._build_key(key)}.parquet")

    def get(
        self, key: str, default: Any | None = None, custom_condition: bool = True
    ) -> Any:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        cache_filename = self._get_cache_filename(key)
        if os.path.exists(cache_filename):
            saved_value = pl.read_parquet( cache_filename).select(0)
            self._log_cache_hit(key, _LOGGER)
            return saved_value
        else:
            self.pop(key)

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

        cache_filename = self._get_cache_filename(key)
        df = pl.DataFrame({"value": [self._build_value(value)]})
        df.write_parquet(cache_filename)

    def pop(self, key: str, custom_condition: bool = True) -> None:
        if not custom_condition:
            self._log_cache_failed(key, _LOGGER)
            return None

        cache_filename = self._get_cache_filename(key)
        if os.path.exists(cache_filename):
            os.remove(cache_filename)
