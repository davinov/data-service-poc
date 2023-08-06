from abc import ABC, abstractmethod
from pydantic import BaseModel

from ..query_plan import QueryPlan


class BaseSource(BaseModel, ABC):
    type: str

    @abstractmethod
    def plan(self) -> QueryPlan:
        """
        Plan the extraction of data from the source.
        """
