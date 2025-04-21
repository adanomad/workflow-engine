# workflow_engine/utils/iter.py
from typing import Sequence, TypeVar


T = TypeVar("T")

def only(it: Sequence[T]) -> T:
    x, = iter(it)
    return x


__all__ = [
    "only",
]
