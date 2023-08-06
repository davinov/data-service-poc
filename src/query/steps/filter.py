from datetime import datetime
from typing import Literal
import polars as pl

from ..plan.in_memory import InMemoryQueryPlan

from ..plan import QueryPlan

from .base import BaseQueryStep
from ..plan.sql import SQLQueryPlan


class FilterStep(BaseQueryStep):
    type: Literal["filter"] = "filter"
    column: str
    value: str | int | float | bool | datetime | None

    def plan(self, pq) -> QueryPlan:
        match pq:
            case SQLQueryPlan() as pq:
                return SQLQueryPlan(
                    connection_uri=pq.connection_uri,
                    query=pq.query.where(pq.table[self.column] == self.value),
                    table=pq.table,
                )
            case InMemoryQueryPlan() as pq:
                return InMemoryQueryPlan(
                    executor=lambda: pq.executor().filter(
                        pl.col(self.column) == self.value
                    )
                )
            case _:
                raise TypeError
