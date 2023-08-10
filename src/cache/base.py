from functools import wraps
from typing import Any, Callable
from sys import getsizeof
import pickle
import time


class BaseCache:
    def __init__(self) -> None:
        ...

    def _log_cache_failed(self, key: str, logger: Any) -> None:
        """Just some logging"""
        logger.debug(">> cache failed on custom_condition")

    def _log_cache_hit(self, key: str, logger: Any) -> None:
        """Just some logging"""
        logger.debug(f">> cache hit for key={key}")

    def _log_cache_missed(self, key: str, logger: Any) -> None:
        """Just some logging"""
        logger.debug(f">> cache missed for key={key}")

    def _size_of(self, value: Any) -> int:
        """To return the memory size of a value"""
        return getsizeof(value)

    def _build_key(self, func: Callable, *args: Any, **kwargs: Any) -> str:
        """The key builder for our cache system"""
        return f"{func.__name__}-{args}-{kwargs}"

    def _build_value(self, value: Any) -> bytes:
        """
        We need to have an uniform way to build the value and how it will be stored
        as bytes for perfs requirements
        """
        return pickle.dumps(
            {
                "value": value,
                "at": time.time(),
                "size_of": self._size_of(value),
            }
        )

    def get(
        self, key: str, default: Any | None = None, custom_condition: bool = True
    ) -> Any:
        """To get something from the cache based on many conditions"""
        ...

    def set(
        self, key: str, value: Any | None = None, custom_condition: bool = True
    ) -> None:
        """To set something based on condition inside the cache"""
        ...

    def get_or_set(
        self, key: str, default: Any | None = None,
        custom_condition: bool = True, call_back: Callable=lambda x: x, *args: Any
    ) -> Any:
        """Get a value from the cache engine or set it and just return the given value"""
        if given_value := self.get(key, default, custom_condition) is None:
            # we proceed with the operation if the value is not in the cache db
            value = call_back(*args)
            self.set(key, value, custom_condition)

            return value

        return given_value


    def pop(self, key: str, custom_condition: bool = True) -> None:
        """To pop something based on condition inside the cache"""
        ...


def cache_dec(cache_engine: BaseCache):
    def decorator(func: Callable) -> Any:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = cache_engine._build_key(func, args, kwargs)

            if cached_value := cache_engine.get(key) is not None:
                return cached_value

            if value := func(*args, **kwargs) is not None:
                cache_engine.set(key, value)
            return value

        return wrapper

    return decorator
