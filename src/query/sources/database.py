from ..query_plan import SQLQueryPlan
from .base import BaseSource


import pypika


from typing import Literal


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
