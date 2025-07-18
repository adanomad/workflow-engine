# workflow_engine/utils/iter.py
from collections.abc import Iterable, Mapping
from typing import Any, Callable, TypeVar

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


def index_by(it: Iterable[T], key: Callable[[T], Any]) -> Mapping[Any, T]:
    acc: dict[Any, T] = {}
    for x in it:
        k = key(x)
        if k in acc:
            raise ValueError(f"Duplicate key {k} in {it}")
        acc[k] = x
    return acc


__all__ = [
    "only",
    "same",
    "index_by",
]
