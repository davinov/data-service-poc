from typing import Literal
import numpy as np
import polars as pl

from .base import BaseQueryStep
from ..query_plan import QueryPlan, InMemoryQueryPlan


class RandomStep(BaseQueryStep):
    """
    Adds a new column with random values
    """

    type: Literal["random"] = "random"
    new_column: str

    def prepare(self, pq) -> QueryPlan:
        match pq:
            case InMemoryQueryPlan() as pq:
                pass
            case _:
                # This step is not supported elsewhere than in memory
                pq = pq.to_memory()

        return InMemoryQueryPlan(
            lazy_frame=pq.lazy_frame.map(
                function=lambda df: df.with_columns(
                    pl.Series(
                        name=self.new_column, values=np.random.uniform(0, 1, len(df))
                    )
                ),
                schema={
                    **pq.lazy_frame.schema,
                    self.new_column: pl.Float64,
                },
            ),
        )
