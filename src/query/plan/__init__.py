from .in_memory import InMemoryQueryPlan
from .sql import SQLQueryPlan


from typing import Union


QueryPlan = Union[
    SQLQueryPlan,
    InMemoryQueryPlan,
]
"""
Represents the work plan to execute a query.
This plan is initialized when reading the source of the query, then
updated on each step.
"""
