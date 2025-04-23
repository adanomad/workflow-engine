# workflow_engine/core/file.py
from abc import ABC
import datetime
import json
from typing import Any, ClassVar, Sequence, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from .context import Context


# HACK: serialize datetime objects
def custom_json_serializer(obj: object) -> Any:
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return None


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


class TextFile(File):
    mime_type = "text/plain"

    def read_text(self, context: "Context") -> str:
        return self.read(context).decode("utf-8")

    def write_text(self, context: "Context", text: str) -> None:
        self.write(context, text.encode("utf-8"))


class JSONFile(TextFile):
    """
    A file that contains a Python object serialized as JSON.
    """
    mime_type = "application/json"

    def read_data(self, context: "Context") -> Any:
        return json.loads(self.read_text(context))

    def write_data(self, context: "Context", data: Any) -> None:
        text = json.dumps(data, default=custom_json_serializer)
        self.write_text(context, text)


class JSONLinesFile(TextFile):
    """
    A file that contains a list of Python objects serialized as JSON.
    """
    mime_type = "application/jsonl"

    def read_data(self, context: "Context") -> Sequence[Any]:
        return [json.loads(line) for line in self.read_text(context).splitlines()]

    def write_data(self, context: "Context", data: Sequence[Any]) -> None:
        text = "\n".join(
            json.dumps(item, default=custom_json_serializer)
            for item in data
        )
        self.write_text(context, text)


__all__ = [
    "File",
    "JSONFile",
    "JSONLinesFile",
    "TextFile",
]
