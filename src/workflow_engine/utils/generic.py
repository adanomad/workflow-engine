import re

_generic_pattern = re.compile(r"^[a-zA-Z]\w+\[.*\]$")


def is_generic(cls: type) -> bool:
    return _generic_pattern.match(cls.__name__) is not None


def get_base(cls: type) -> type:
    while is_generic(cls):
        assert cls.__base__ is not None
        cls = cls.__base__
    return cls


__all__ = [
    "get_base",
    "is_generic",
]
