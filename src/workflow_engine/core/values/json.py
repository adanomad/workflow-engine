# workflow_engine/core/values/json.py

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from .value import Value

if TYPE_CHECKING:
    from ..context import Context


type JSON = Mapping[str, JSON] | Sequence[JSON] | None | bool | int | float | str


class JSONValue(Value[JSON]):
    pass


@Value.register_cast_to(JSONValue)
def cast_any_to_json(value: Value, context: "Context") -> JSONValue:
    return JSONValue(value.model_dump())


__all__ = [
    "JSON",
    "JSONValue",
]
