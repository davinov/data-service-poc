from query.sources.database import DatabaseSource


from pydantic import Field


from typing import Annotated, Union

from query.sources.file import FileSource


QuerySource = Annotated[Union[FileSource, DatabaseSource], Field(discriminator="type")]
