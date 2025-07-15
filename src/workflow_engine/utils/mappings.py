# workflow_engine/utils/mappings.py

from collections.abc import Mapping

from functools import reduce
from typing import TypeVar

from .iter import same

K = TypeVar("K")
V = TypeVar("V")


def mapping_intersection(
    *mappings: Mapping[K, V],
) -> Mapping[K, V]:
    """
    Computes the intersection of the given mappings, which consists of the keys
    in common to all mappings.

    For each key in the intersection, the associated value must be the same
    across all mappings.
    """
    if len(mappings) == 0:
        return {}
    if len(mappings) == 1:
        return mappings[0]

    keys = reduce(
        lambda acc, mapping: acc & set(mapping.keys()),
        mappings[1:],
        set(mappings[0].keys()),
    )
    return {key: same(mapping[key] for mapping in mappings) for key in keys}


__all__ = [
    "mapping_intersection",
]
