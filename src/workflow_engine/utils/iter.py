# workflow_engine/utils/iter.py
from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def only(it: Iterable[T]) -> T:
    (x,) = iter(it)
    return x


def same(it: Iterable[T]) -> T:
    it = iter(it)
    x = next(it)
    for y in it:
        if x != y:
            raise ValueError("Values are not the same")
    return x


__all__ = [
    "only",
    "same",
]
