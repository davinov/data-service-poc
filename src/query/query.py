import logging
import polars as pl
from pydantic import BaseModel

from .performance_utils import Timer
from .query_source import QueryPlan, QuerySource
from .steps import QueryStep


class Query(BaseModel):
    source: QuerySource
    steps: list[QueryStep]

    def plan(self) -> QueryPlan:
        pq = self.source.plan()
        for step in self.steps:
            pq = step.plan(pq)
        return pq

    def execute(self) -> pl.DataFrame:  # TODO maybe return pyarrow Table instead
        logging.debug(f'Start query planning')
        timer_plan = Timer()
        with timer_plan:
            plan = self.plan()
        logging.debug(f'End of query planning - duration: {timer_plan}')
        
        logging.debug(f'Start query execution')
        timer_exec = Timer()
        with timer_exec:
            result = plan.execute()
        logging.debug(f'End query execution - duration: {timer_exec}')
        return result
