from abc import ABC, abstractmethod
from typing import Annotated, Literal, Union
from pydantic import BaseModel, Field, FilePath
import polars as pl
import pypika.queries

from .query_plan import QueryPlan, InMemoryQueryPlan, SQLQueryPlan


class BaseSource(BaseModel, ABC):
    type: str

    @abstractmethod
    def prepare(self) -> QueryPlan:
        """
        Prepare the source to a format where steps can be added.
        If data can't be further transformed in the source, the result would be
        polars' LazyFrame.
        If more transformations would be pushed in the source engine, the result
        will be a query for this engine, to be completed.
        """


class FileSource(BaseSource):
    type: Literal["file"] = "file"
    file: FilePath

    def prepare(self):
        return InMemoryQueryPlan(executor=lambda: pl.scan_csv(self.file))


class DatabaseSource(BaseSource):
    type: Literal["database"] = "database"
    connection_uri: str
    table: str

    def prepare(self):
        table = pypika.Table(self.table)
        return SQLQueryPlan(
            connection_uri=self.connection_uri,
            table=table,
            query=pypika.Query.from_(table),
        )


QuerySource = Annotated[Union[FileSource, DatabaseSource], Field(discriminator="type")]
