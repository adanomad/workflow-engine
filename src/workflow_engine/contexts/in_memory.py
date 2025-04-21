# workflow_engine/contexts/in_memory.py
from ..core import Context, File


class InMemoryContext(Context):
    """
    Pretends to be a file system, but actually stores files in memory.
    """
    def __init__(self, run_id: str):
        super().__init__(run_id)
        self.data: dict[str, bytes] = {}

    def read(
            self,
            file: File,
    ) -> bytes:
        return self.data[file.path]

    def write(
            self,
            file: File,
            content: bytes,
    ) -> None:
        self.data[file.path] = content


__all__ = [
    "InMemoryContext",
]
