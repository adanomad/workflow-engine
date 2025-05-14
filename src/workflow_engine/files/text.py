# workflow_engine/files/text.py
from typing import Self

from ..core import Context, File


class TextFile(File):
    mime_type = "text/plain"

    async def read_text(self, context: "Context") -> str:
        return (await self.read(context)).decode("utf-8")

    async def write_text(self, context: "Context", text: str) -> Self:
        return await self.write(context, text.encode("utf-8"))


__all__ = [
    "TextFile",
]
