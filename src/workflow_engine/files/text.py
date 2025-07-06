# workflow_engine/files/text.py
from typing import Self

from ..core import Context, FileValue, StringValue


class TextFileValue(FileValue):
    async def read_text(self, context: "Context") -> str:
        return (await self.read(context)).decode("utf-8")

    async def write_text(self, context: "Context", text: str) -> Self:
        return await self.write(context, text.encode("utf-8"))


@TextFileValue.register_cast_to(StringValue)
async def cast_text_to_string(value: TextFileValue, context: "Context") -> StringValue:
    return StringValue(await value.read_text(context))


__all__ = [
    "TextFileValue",
]
