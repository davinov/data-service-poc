from ..performance_utils import Timer
from .in_memory import InMemoryQueryPlan
from .base import BaseQueryPlan


import polars as pl
import pypika.queries


import logging


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
            logging.debug(
                f"""
                    Sending SQL query:
                        {sql_query}
                """)
            df = pl.read_database(sql_query, self.connection_uri)
        logging.debug(
                f"""
                    SQL query duration: {timer}
                """)
        return df

    def to_memory(self) -> InMemoryQueryPlan:
        """
        Utility to switch towards in-memory processing when no other method is available
        """
        return InMemoryQueryPlan(executor=lambda: self.execute().lazy())
