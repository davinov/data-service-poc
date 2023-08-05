from datetime import datetime
from typing import Literal
import polars as pl

from .base import BaseQueryStep
from ..query_plan import QueryPlan, SQLQueryPlan, InMemoryQueryPlan


class FilterStep(BaseQueryStep):
    type: Literal["filter"] = "filter"
    column: str
    value: str | int | float | bool | datetime | None

    def prepare(self, pq) -> QueryPlan:
        match pq:
            case SQLQueryPlan() as pq:
                return SQLQueryPlan(
                    connection_uri=pq.connection_uri,
                    query=pq.query.where(pq.table[self.column] == self.value),
                    table=pq.table,
                )
            case InMemoryQueryPlan() as pq:
                return InMemoryQueryPlan(
                    lazy_frame=pq.lazy_frame.filter(pl.col(self.column) == self.value)
                )
            case _:
                raise TypeError
