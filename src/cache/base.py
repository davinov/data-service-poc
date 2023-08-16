from abc import ABC, abstractmethod, abstractproperty
from functools import wraps
from hashlib import md5
from typing import Any, Callable
from sys import getsizeof
import pickle
import time


class BaseCache(ABC):
    """
    the dumper and the loader
    defined how the subclass implement the way
    the data is dumps/load and can be updated
    """

    @abstractproperty
    def dumper(something: Any):
        return pickle.dumps

    @abstractproperty
    def loader(something: Any):
        return pickle.loads

    def _log_cache_failed(self, key: str, logger: Any) -> None:
        """Just some logging"""
        logger.debug(f">> cache failed on custom_condition for {key=}")

    def _log_cache_hit(self, key: str, logger: Any) -> None:
        """Just some logging"""
        logger.debug(f">> cache hit for {key=}")

    def _log_cache_missed(self, key: str, logger: Any) -> None:
        """Just some logging"""
        logger.debug(f">> cache missed for {key=}")

    def _size_of(self, value: Any) -> int:
        """To return the memory size of a value"""
        return getsizeof(value)

    def _build_key(self, *args: Any, **kwargs: Any) -> str:
        """The key builder for our cache system"""
        return md5(f"{args}-{kwargs}".encode("utf-8")).hexdigest()

    def _build_value(self, value: Any) -> bytes:
        """
        We need to have an uniform way to build the value and how it will be stored
        as bytes for perfs requirements
        """
        return self.dumper(
            {
                "value": value,
                "at": time.time(),
                "size_of": self._size_of(value),
            }
        )

    def _load_value(self, value: Any) -> Any:
        return self.loader(value)

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
        self,
        key: str,
        default: Any | None = None,
        custom_condition: bool = True,
        call_back: Callable = lambda x: x,
        *args: Any,
    ) -> Any:
        """
        Get a value from the cache engine or set it and just return the given value
        """
        if given_value := self.get(key, default, custom_condition) is None:
            # we proceed with the operation if the value is not in the cache db
            value = call_back(*args)
            self.set(key, value, custom_condition)

            return value

        return given_value

    @abstractmethod
    def pop(self, key: str, custom_condition: bool = True) -> None:
        """To pop something based on condition inside the cache"""
        ...


# df = use_cache(
#   cache_type='sql_query',
#   cache_key=[sql_query, self.connection_uri],
#   method=lambda: pl.read_database(sql_query, self.connection_uri)
# )
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
