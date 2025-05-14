# workflow_engine/contexts/in_memory.py
from ..core import Context, File
from typing import TypeVar


F = TypeVar("F", bound=File)


class InMemoryContext(Context):
    """
    Pretends to be a file system, but actually stores files in memory.
    """

    def __init__(self, *, run_id: str | None = None):
        super().__init__(run_id=run_id)
        self.data: dict[str, bytes] = {}

    async def read(
        self,
        file: File,
    ) -> bytes:
        return self.data[file.path]

    async def write(
        self,
        file: F,
        content: bytes,
    ) -> F:
        self.data[file.path] = content
        return file


__all__ = [
    "InMemoryContext",
]
