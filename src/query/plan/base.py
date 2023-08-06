import polars as pl


from abc import ABC, abstractmethod


class BaseQueryPlan(ABC):
    @abstractmethod
    def execute(self) -> pl.DataFrame:
        """
        Retrieve the results of the query
        """
