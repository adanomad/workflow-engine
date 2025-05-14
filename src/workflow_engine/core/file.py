# workflow_engine/core/file.py
from abc import ABC
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from .context import Context


class File(BaseModel, ABC):
    """
    A serializable reference to a file.

    A Context provides the actual implementation to read the file's contents.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")
    metadata: Mapping[str, Any] = Field(default_factory=dict)
    mime_type: ClassVar[str]
    path: str

    async def read(self, context: "Context") -> bytes:
        return await context.read(file=self)

    async def write(self, context: "Context", content: bytes) -> Self:
        return await context.write(file=self, content=content)

    def write_metadata(self, key: str, value: Any) -> Self:
        if key in self.metadata:
            assert self.metadata[key] == value
            return self
        metadata = dict(self.metadata)
        metadata[key] = value
        return self.model_copy(update={"metadata": metadata})


__all__ = [
    "File",
]
