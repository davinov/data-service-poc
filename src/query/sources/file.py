from ..plan.in_memory import InMemoryQueryPlan
from .base import BaseSource

import polars as pl
from pydantic import FilePath

from typing import Literal


class FileSource(BaseSource):
    type: Literal["file"] = "file"
    file: FilePath

    def plan(self):
        return InMemoryQueryPlan(executor=lambda: pl.scan_csv(self.file))
