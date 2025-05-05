# src/workflow_engine/utils/assign.py
from enum import Enum
from types import NoneType, UnionType
from typing import (
    Any,
    Literal,
    Optional,
    TypeAlias,
    Union,
    cast,
    get_args,
    get_origin,
)

from .iter import only

LiteralType: TypeAlias = int | bool | str | bytes | Enum | NoneType

ExpandedType: TypeAlias = tuple[bool, Any]


def expand_type(t: Any) -> list[ExpandedType]:
    """
    Expands the type into an iterable of (is_literal, x) pairs, where
    - if is_literal is True, then x is a LiteralType instance
    - if is_literal is False, then x is a non-Any, non-None, non-Literal,
      non-Optional, non-Union type.
    """
    if t is None or t is NoneType:
        # it's more useful to have None than NoneType since None can be checked
        # for equality with literals
        return [(True, None)]
    if t is Any:  # resolve the horribly wrong Any = object() type alias
        return [(False, object)]

    if (origin := get_origin(t)) is not None:
        args = get_args(t)
        if origin is Optional:
            return [(True, None)] + expand_type(only(args))
        if origin is Literal:
            return [(True, a) for a in args]
        if origin is Union or origin is UnionType:
            return sum((expand_type(a) for a in args), [])
    return [(False, t)]


def safe_issubclass(
    source: Any,
    target: Any,
) -> bool:
    try:
        return issubclass(cast(type, source), cast(type, target))
    except TypeError:
        return source == target


def is_assignable_expanded(
    expanded_source: ExpandedType,
    expanded_target: ExpandedType,
    covariant: bool = False,
) -> bool:
    """
    Check if a source object or type is assignable to a target object or type.
    """
    source_is_literal, source = expanded_source
    target_is_literal, target = expanded_target

    if source_is_literal:
        if target_is_literal:
            # type check ensures we don't get false equality between an enum case
            # and a literal of the same value
            return (type(source) is type(target)) and (source == target)
        if get_origin(target) is None:
            # literals are only assignable to non-generic types
            return isinstance(source, target)
        return False

    if target_is_literal:
        return False

    # anything is assignable to object
    if target is object:
        return True

    source_origin = get_origin(source)
    target_origin = get_origin(target)
    source_args = get_args(source)
    target_args = get_args(target)

    # WARNING: beyond this point we are relying on unsafe assumptions

    # if both are generic types
    if source_origin is None:
        if target_origin is None:
            return safe_issubclass(source, target)
        return False

    if target_origin is None:
        return False

    # ASSUMES: a generic subtype of another generic is a subtype if its arguments match
    if not safe_issubclass(source_origin, target_origin):
        return False
    # ASSUMES: generic type parameters are always provided
    assert len(source_args) == len(target_args)

    # If covariant is True, check if source args are subtypes of target args
    # Otherwise, require exact type matches
    if covariant:
        # For covariant type checking, we need to check if each source arg is assignable to the corresponding target arg
        # This allows List[int] to be assignable to List[float|int]
        return all(
            is_assignable(s, t, covariant=True)
            for s, t in zip(source_args, target_args)
        )
    else:
        # For invariant type checking, we require exact type matches
        return all(s == t for s, t in zip(source_args, target_args))


def is_assignable(
    source: Any,
    target: Any,
    *,
    covariant: bool = False,
) -> bool:
    """
    Check if a source object or type is assignable to a target object or type.
    Does not handle:
      - callables
      - type variables
      - concretely typed subtypes of generic types

    Args:
        source: The type of the value being assigned.
        target: The type of the variable to assign to.
        covariant: If True, treat generic types as covariant, meaning that
                   A[X] is assignable to A[Y] if X is assignable to Y.

    Returns:
        bool: True if source is assignable to target, False otherwise
    """
    source_expanded = expand_type(source)
    target_expanded = expand_type(target)

    return all(
        any(is_assignable_expanded(s, t, covariant=covariant) for t in target_expanded)
        for s in source_expanded
    )


__all__ = [
    "is_assignable",
]
