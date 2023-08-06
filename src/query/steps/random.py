from typing import Literal
import numpy as np
import polars as pl

from ..plan import QueryPlan

from .base import BaseQueryStep
from ..plan.in_memory import InMemoryQueryPlan


class RandomStep(BaseQueryStep):
    """
    Adds a new column with random values
    """

    type: Literal["random"] = "random"
    new_column: str

    def plan(self, pq) -> QueryPlan:
        match pq:
            case InMemoryQueryPlan() as pq:
                pass
            case _:
                # This step is not supported elsewhere than in memory
                pq = pq.to_memory()

        def execute_random_step():
            lazy_frame = pq.executor()
            return lazy_frame.map(
                function=lambda df: df.with_columns(
                    pl.Series(
                        name=self.new_column, values=np.random.uniform(0, 1, len(df))
                    )
                ),
                schema={
                    **lazy_frame.schema,
                    self.new_column: pl.Float64,
                },
            )

        return InMemoryQueryPlan(executor=execute_random_step)
