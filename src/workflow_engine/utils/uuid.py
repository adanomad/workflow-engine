# workflow_engine/utils/uuid.py
from uuid import UUID


# https://stackoverflow.com/a/79298779
def is_valid_uuid(s: str, version: int = 4) -> bool:
    """
    Checks if s is a serialized UUID by attempting to round-trip deserialize and
    serialize it.
    """
    try:
        return s == str(UUID(s, version=version))
    except ValueError:
        return False


__all__ = [
    "is_valid_uuid",
]
