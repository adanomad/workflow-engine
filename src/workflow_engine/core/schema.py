# src/workflow_engine/core/schema.py
"""
Implements a subset of the JSON Schema specification that we support.

You may be surprised that there is no library for parsing and manipulating JSON
schema as Python objects.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from functools import cached_property
from typing import Any, Literal, Type, TypeAlias

from overrides import override
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .data import Data, DataValue, build_data_type
from .value import (
    BooleanValue,
    FloatValue,
    IntegerValue,
    NullValue,
    SequenceValue,
    StringMapValue,
    StringValue,
    Value,
    ValueType,
    _value_registry,
)


class BaseJSONSchema(ABC, BaseModel):
    """
    https://json-schema.org/understanding-json-schema/reference/schema
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    @property
    @abstractmethod
    def value_type(self) -> ValueType:
        raise NotImplementedError()


class IntegerJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/numeric
    """

    type: Literal["integer"]
    minimum: int | None = None
    maximum: int | None = None
    multipleOf: int | None = None

    @model_validator(mode="after")
    def validate_range(self):
        if self.minimum is not None and self.maximum is not None:
            if self.minimum > self.maximum:
                raise ValueError(
                    f"Invalid range: minimum ({self.minimum}) cannot exceed maximum ({self.maximum})."
                )
        return self

    @property
    @override
    def value_type(self) -> ValueType:
        return IntegerValue


class NumberJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/numeric
    """

    type: Literal["number"]
    minimum: float | None = None
    maximum: float | None = None
    multipleOf: float | None = None

    @model_validator(mode="after")
    def validate_range(self):
        if self.minimum is not None and self.maximum is not None:
            if self.minimum > self.maximum:
                raise ValueError(
                    f"Invalid range: minimum ({self.minimum}) cannot exceed maximum ({self.maximum})."
                )
        return self

    @property
    @override
    def value_type(self) -> ValueType:
        return FloatValue


class BooleanJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/boolean
    """

    type: Literal["boolean"]

    @property
    @override
    def value_type(self) -> ValueType:
        return BooleanValue


class NullJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/null
    """

    type: Literal["null"]

    @property
    @override
    def value_type(self) -> ValueType:
        return NullValue


class StringJSONSchema(BaseJSONSchema):
    type: Literal["string"]
    minLength: int | None = None
    maxLength: int | None = None
    pattern: str | None = None

    @model_validator(mode="after")
    def validate_range(self):
        if self.minLength is not None and self.maxLength is not None:
            assert self.minLength <= self.maxLength, (
                f"minLength ({self.minLength}) cannot be greater than maxLength ({self.maxLength})."
            )
        return self

    @property
    @override
    def value_type(self) -> ValueType:
        return StringValue


class SequenceJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/array
    """

    type: Literal["array"]
    items: JSONSchemaUnion
    minItems: int | None = None
    maxItems: int | None = None
    uniqueItems: bool | None = None

    @property
    @override
    def value_type(self) -> ValueType:
        return SequenceValue[self.items.value_type]


class StringMapJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/object
    """

    type: Literal["object"]
    additionalProperties: JSONSchemaUnion

    @property
    @override
    def value_type(self) -> ValueType:
        return StringMapValue[self.additionalProperties.value_type]


class ObjectJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/object
    """

    type: Literal["object"]
    properties: Mapping[str, JSONSchemaUnion]
    required: Set[str] = Field(default_factory=set)
    additionalProperties: bool = False

    @cached_property
    def data_type(self) -> Type[Data]:
        return build_data_type(
            "ObjectData",
            {k: (v.value_type, k in self.required) for k, v in self.properties.items()},
        )

    @property
    @override
    def value_type(self) -> ValueType:
        return DataValue[self.data_type]


class JSONSchemaRef(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/json-pointer

    Assumes that all of the Value types are defined, each with a unique class
    name.
    For example, { "$ref": "IntegerValue" } resolves to IntegerValue.
    """

    ref: str = Field(..., alias="$ref")

    @cached_property
    def name(self) -> str:
        return self.ref.removeprefix("#/$defs/")

    @model_validator(mode="after")
    def validate_data_type(self):
        assert self.ref.startswith("#/$defs/")
        return self

    @property
    @override
    def value_type(self) -> ValueType:
        return _value_registry.get(self.name)

    @staticmethod
    def from_ref(ref: str) -> JSONSchemaRef:
        return JSONSchemaRef(**{"$ref": ref})

    @staticmethod
    def from_name(name: str) -> JSONSchemaRef:
        return JSONSchemaRef.from_ref(f"#/$defs/{name}")


JSONSchemaUnion: TypeAlias = (
    BooleanJSONSchema
    | IntegerJSONSchema
    | JSONSchemaRef
    | NullJSONSchema
    | NumberJSONSchema
    | ObjectJSONSchema
    | SequenceJSONSchema
    | StringJSONSchema
    | StringMapJSONSchema
)


class JSONSchemaValue(Value[JSONSchemaUnion]):
    """
    A wrapper class to allow users to read any of the JSONSchemaUnion types.
    """

    @staticmethod
    def loads(s: str) -> JSONSchemaUnion:
        """
        Convert a JSON string to a JSONSchemaUnion.
        """
        return JSONSchemaValue.model_validate_json(s).root

    @staticmethod
    def load(d: Mapping[str, Any]) -> JSONSchemaUnion:
        """
        Convert a Python dictionary object to a JSONSchemaUnion.
        """
        return JSONSchemaValue.loads(json.dumps(d))


__all__ = [
    "JSONSchemaValue",
]
