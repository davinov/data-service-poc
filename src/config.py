from functools import lru_cache


from cache.in_memory import InMemoryCache


class Settings:
    CACHE = InMemoryCache()
    USE_CACHE = True

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
