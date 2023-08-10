import logging

from .base import BaseQueryPlan


import polars as pl


from typing import Callable


class InMemoryQueryPlan(BaseQueryPlan):
    """
    Represents operations made on a Lazy DataFrame with polars.
    As we don't want to execute them right away, but just to plan them, we
    wrap them in a function to be executed later.

    Working with LazyFrame, instead of DataFrame, allows polars to make
    optimizations just before executing the whole pipeline.
    """

    def __init__(self, executor: Callable[[], pl.LazyFrame]):
        self.executor = executor

    def execute(self) -> pl.DataFrame:
        df, profile_info = self.executor().profile()
        logging.debug(
            f"""
                In memory processing finished
                    - profile: {profile_info}
            """
        )
        return df
