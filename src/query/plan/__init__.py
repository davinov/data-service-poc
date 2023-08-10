from typing import Union
from query.plan.in_memory import InMemoryQueryPlan

from query.plan.sql import SQLQueryPlan


QueryPlan = Union[
    SQLQueryPlan,
    InMemoryQueryPlan,
]
"""
Represents the work plan to execute a query.
This plan is initialized when reading the source of the query, then
updated on each step.
"""
