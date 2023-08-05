from typing import Annotated, Union
from pydantic import Field

from .filter import FilterStep
from .join import JoinStep
from .random import RandomStep

QueryStep = Annotated[
    Union[FilterStep, JoinStep, RandomStep], Field(discriminator="type")
]
