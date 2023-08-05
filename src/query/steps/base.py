from abc import ABC, abstractmethod
from pydantic import BaseModel
import polars as pl

from ..query_source import QueryPlan


class BaseQueryStep(BaseModel, ABC):
    type: str

    @abstractmethod
    def prepare(self, pq: QueryPlan) -> QueryPlan:
        """
        Apply the current step to the prepared query
        """
