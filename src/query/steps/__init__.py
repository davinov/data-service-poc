from typing import Annotated, Union
from pydantic import Field

from query.steps.filter import FilterStep
from query.steps.join import JoinStep
from query.steps.random import RandomStep

QueryStep = Annotated[
    Union[FilterStep, JoinStep, RandomStep], Field(discriminator="type")
]
