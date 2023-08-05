import polars as pl
from pydantic import BaseModel

from .query_source import QueryPlan, QuerySource
from .steps import QueryStep


class Query(BaseModel):
    source: QuerySource
    steps: list[QueryStep]

    def prepare(self) -> QueryPlan:
        pq = self.source.prepare()
        for step in self.steps:
            pq = step.prepare(pq)
        return pq

    def execute(self) -> pl.DataFrame:  # TODO maybe return pyarrow Table instead
        return self.prepare().execute()
