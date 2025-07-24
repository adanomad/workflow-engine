# workflow_engine/files/pdf.py
from typing import ClassVar

from ..core import FileValue


class PDFFileValue(FileValue):
    mime_type: ClassVar[str] = "application/pdf"


__all__ = [
    "PDFFileValue",
]
