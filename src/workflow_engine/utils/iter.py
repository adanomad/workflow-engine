# workflow_engine/utils/iter.py
from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def only(it: Iterable[T]) -> T:
    (x,) = iter(it)
    return x


__all__ = [
    "only",
]
