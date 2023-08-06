import logging
import polars as pl
from pydantic import BaseModel

from .query_source import QueryPlan, QuerySource
from .steps import QueryStep


class Query(BaseModel):
    source: QuerySource
    steps: list[QueryStep]

    def plan(self) -> QueryPlan:
        logging.debug('Start query plan')
        pq = self.source.plan()
        for step in self.steps:
            pq = step.plan(pq)
        logging.debug('End query plan')
        return pq


    def execute(self) -> pl.DataFrame:  # TODO maybe return pyarrow Table instead
        logging.debug('Start query execution')
        result = self.plan().execute()
        logging.debug('End of query execution')
        return result
