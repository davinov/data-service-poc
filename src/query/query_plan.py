from abc import ABC, abstractmethod
import logging
from typing import Callable, Union
import polars as pl
import pypika.queries

from .performance_utils import Timer


class BaseQueryPlan(ABC):
    @abstractmethod
    def execute(self) -> pl.DataFrame:
        """
        Retrieve the results of the query
        """


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
        return self.executor().collect()


class SQLQueryPlan(BaseQueryPlan):
    """
    Represents a SQL query, being built with pypika
    """

    def __init__(
        self,
        connection_uri: str,
        query: pypika.queries.QueryBuilder,
        table: pypika.queries.Selectable,
    ):
        self.connection_uri = connection_uri
        self.query = query
        self.table = table

    def execute(self) -> pl.DataFrame:
        sql_query = self.query.select("*").get_sql()
        timer = Timer()
        with timer:
            logging.debug(f'Sending SQL query: {sql_query}')
            df = pl.read_database(sql_query, self.connection_uri)
        logging.debug(f'SQL query duration: {timer}')
        return df

    def to_memory(self) -> InMemoryQueryPlan:
        """
        Utility to switch towards in-memory processing when no other method is available
        """
        return InMemoryQueryPlan(executor=lambda: self.execute().lazy())


QueryPlan = Union[
    SQLQueryPlan,
    InMemoryQueryPlan,
]
"""
Represents the work plan to execute a query.
This plan is initialized when reading the source of the query, then
updated on each step.
"""
