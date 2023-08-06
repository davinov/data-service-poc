from abc import ABC, abstractmethod
from typing import Annotated, Literal, Union
from pydantic import BaseModel, Field, FilePath
import polars as pl
import pypika.queries

from .query_plan import QueryPlan, InMemoryQueryPlan, SQLQueryPlan


class BaseSource(BaseModel, ABC):
    type: str

    @abstractmethod
    def plan(self) -> QueryPlan:
        """
        Plan the extraction of data from the source.
        """


class FileSource(BaseSource):
    type: Literal["file"] = "file"
    file: FilePath

    def plan(self):
        return InMemoryQueryPlan(executor=lambda: pl.scan_csv(self.file))


class DatabaseSource(BaseSource):
    type: Literal["database"] = "database"
    connection_uri: str
    table: str

    def plan(self):
        table = pypika.Table(self.table)
        return SQLQueryPlan(
            connection_uri=self.connection_uri,
            table=table,
            query=pypika.Query.from_(table),
        )


QuerySource = Annotated[Union[FileSource, DatabaseSource], Field(discriminator="type")]
