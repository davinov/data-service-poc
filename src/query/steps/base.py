from abc import ABC, abstractmethod
from pydantic import BaseModel

from ..plan import QueryPlan


class BaseQueryStep(BaseModel, ABC):
    type: str

    @abstractmethod
    def plan(self, pq: QueryPlan) -> QueryPlan:
        """
        Adds the current step to a query plan
        """
