# workflow_engine/files/json.py
import datetime
import json
from collections.abc import Sequence
from typing import Any, Self

from ..core import Context
from .text import TextFile


# HACK: serialize datetime objects
def _custom_json_serializer(obj: object) -> Any:
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return None


class JSONFile(TextFile):
    """
    A file that contains a Python object serialized as JSON.
    """

    mime_type = "application/json"

    async def read_data(self, context: "Context") -> Any:
        return json.loads(await self.read_text(context))

    async def write_data(self, context: "Context", data: Any) -> Self:
        text = json.dumps(data, default=_custom_json_serializer)
        return await self.write_text(context, text)


class JSONLinesFile(TextFile):
    """
    A file that contains a list of Python objects serialized as JSON.
    """

    mime_type = "application/jsonl"

    async def read_data(self, context: "Context") -> Sequence[Any]:
        return [
            json.loads(line) for line in (await self.read_text(context)).splitlines()
        ]

    async def write_data(self, context: "Context", data: Sequence[Any]) -> Self:
        text = "\n".join(
            json.dumps(item, default=_custom_json_serializer) for item in data
        )
        return await self.write_text(context, text)


__all__ = [
    "JSONFile",
    "JSONLinesFile",
]
