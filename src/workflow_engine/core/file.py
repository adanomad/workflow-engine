# workflow_engine/core/file.py
from abc import ABC
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import BaseModel, ConfigDict, Field

from .value import Value

if TYPE_CHECKING:
    from .context import Context


class File(BaseModel, ABC):
    """
    A serializable reference to a file.

    A Context provides the actual implementation to read the file's contents.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    metadata: Mapping[str, Any] = Field(default_factory=dict)
    mime_type: ClassVar[str]
    path: str


class FileValue(Value[File]):
    """
    A Value that represents a file.
    """

    async def read(self, context: "Context") -> bytes:
        return await context.read(file=self)

    async def write(self, context: "Context", content: bytes) -> Self:
        return await context.write(file=self, content=content)

    async def copy_from_local_file(self, context: "Context", path: str) -> Self:
        with open(path, "rb") as f:
            data = f.read()
            return await self.write(context, data)

    def write_metadata(self, key: str, value: Any) -> Self:
        if key in self.root.metadata:
            assert self.root.metadata[key] == value
            return self
        metadata = dict(self.root.metadata)
        metadata[key] = value
        return type(self)(self.root.model_copy(update={"metadata": metadata}))

    @classmethod
    def from_path(cls, path: str, **metadata: Any) -> Self:
        return cls(root=File(path=path, metadata=metadata))


__all__ = [
    "File",
    "FileValue",
]
