# workflow_engine/core/file.py
from abc import ABC
import json
from typing import Any, ClassVar, Sequence, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, field_validator

if TYPE_CHECKING:
    from .context import Context


class File(BaseModel, ABC):
    """
    A serializable reference to a file.

    A Context provides the actual implementation to read the file's contents.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")
    mime_type: ClassVar[str]
    path: str

    def read(self, context: "Context") -> bytes:
        return context.read(file=self)

    def write(self, context: "Context", content: bytes) -> None:
        context.write(file=self, content=content)

    @field_validator("path")
    def _validate_extension(cls, v: str) -> str:
        return v


class TextFile(File):
    mime_type = "text/plain"

    def read_text(self, context: "Context") -> str:
        return self.read(context).decode("utf-8")

    def write_text(self, context: "Context", text: str) -> None:
        self.write(context, text.encode("utf-8"))

    @field_validator("path")
    def _validate_extension(cls, v: str) -> str:
        return v if v.endswith(".txt") else f"{v}.txt"


class JSONFile(TextFile):
    """
    A file that contains a Python object serialized as JSON.
    """
    mime_type = "application/json"

    def read_data(self, context: "Context") -> Any:
        return json.loads(self.read_text(context))

    def write_data(self, context: "Context", data: Any) -> None:
        self.write_text(context, json.dumps(data))

    @field_validator("path")
    def _validate_extension(cls, v: str) -> str:
        return v if v.endswith(".json") else f"{v}.json"


class JSONLinesFile(TextFile):
    """
    A file that contains a list of Python objects serialized as JSON.
    """
    mime_type = "application/jsonl"

    def read_data(self, context: "Context") -> Sequence[Any]:
        return [json.loads(line) for line in self.read_text(context).splitlines()]

    def write_data(self, context: "Context", data: Sequence[Any]) -> None:
        self.write_text(context, "\n".join(json.dumps(item) for item in data))

    @field_validator("path")
    def _validate_extension(cls, v: str) -> str:
        return v if v.endswith(".jsonl") else f"{v}.jsonl"


__all__ = [
    "File",
    "JSONFile",
    "JSONLinesFile",
    "TextFile",
]
