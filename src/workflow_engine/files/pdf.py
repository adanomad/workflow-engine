# workflow_engine/files/pdf.py
from typing import Self

from ..core import Context, File


class PDFFile(File):
    mime_type = "application/pdf"

    async def copy_from_local_file(self, context: Context, path: str) -> Self:
        with open(path, "rb") as f:
            data = f.read()
            return await self.write(context, data)


__all__ = [
    "PDFFile",
]
