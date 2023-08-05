from typing import TYPE_CHECKING, Literal

from .base import BaseQueryStep
from ..query_plan import QueryPlan, SQLQueryPlan, InMemoryQueryPlan

if TYPE_CHECKING:
    from ..query import Query


class JoinStep(BaseQueryStep):
    type: Literal["join"] = "join"
    right_query: "Query"
    on: str

    def prepare(self, pq) -> QueryPlan:
        right_query_pq = self.right_query.prepare()

        # If both queries are SQL queries from the same DB,
        # then we can use a JOIN clause in the SQL query
        if (
            isinstance(right_query_pq, SQLQueryPlan)
            and isinstance(pq, SQLQueryPlan)
            and pq.connection_uri == right_query_pq.connection_uri
        ):
            return SQLQueryPlan(
                connection_uri=pq.connection_uri,
                table=pq.table,
                query=pq.query.join(right_query_pq.query.select("*")).on_field(self.on),
            )

        # For now, only support in memory
        if not isinstance(right_query_pq, InMemoryQueryPlan):
            right_query_pq = right_query_pq.to_memory()

        if not isinstance(pq, InMemoryQueryPlan):
            pq = pq.to_memory()

        return InMemoryQueryPlan(
            lazy_frame=pq.lazy_frame.join(
                other=right_query_pq.lazy_frame,
                on=self.on,
            )
        )
