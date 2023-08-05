from abc import ABC, abstractmethod
from typing import Union
import polars as pl
import pypika.queries


class BaseQueryPlan(ABC):
    @abstractmethod
    def execute(self) -> pl.DataFrame:
        """
        Retrieve the results of the query
        """


class InMemoryQueryPlan(BaseQueryPlan):
    """
    Represents operations made on a Lazy DataFrame with polars
    """

    def __init__(self, lazy_frame: pl.LazyFrame):
        self.lazy_frame = lazy_frame

    def execute(self) -> pl.DataFrame:
        return self.lazy_frame.collect()


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
        print("SQL QUERY:", self.query.select("*").get_sql())
        return pl.read_database(self.query.select("*").get_sql(), self.connection_uri)

    def to_memory(self) -> InMemoryQueryPlan:
        """
        Utility to switch towards in-memory processing when no other method is available
        """
        return InMemoryQueryPlan(
            lazy_frame=self.execute().lazy()
            # FIXME how can we delay execution?
            # Idea: see if we wan implement a Scanner for read_database
            # (We may just need the schema)
            # See this issue https://github.com/pola-rs/polars/issues/4351,
            # the PythonScanExec https://github.com/pola-rs/polars/blob/1e3f703698099faff5cddd2d96e4cbd52944bb93/polars/polars-lazy/src/physical_plan/executors/python_scan.rs#L19
            # and its counterpart in Python https://github.com/pola-rs/polars/blob/main/py-polars/polars/io/ipc/anonymous_scan.py#L24
        )


QueryPlan = Union[
    SQLQueryPlan,
    InMemoryQueryPlan,
]
"""
Represents the work plan to execute a query.
This plan is initialized when reading the source of the query, then
updated on each step.
"""
