import polars as pl
from pydantic import FilePath

from typing import Literal
from query.plan.in_memory import InMemoryQueryPlan

from query.sources.base import BaseSource


class FileSource(BaseSource):
    type: Literal["file"] = "file"
    file: FilePath

    def plan(self):
        return InMemoryQueryPlan(executor=lambda: pl.scan_csv(self.file))
