# src/workflow_engine/utils/schema.py
"""
Implements a subset of the JSON Schema specification that we support.

You may be surprised that there is no library for parsing and manipulating JSON
schema as Python objects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from functools import cached_property
from typing import Literal, Type

from overrides import override
from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator

from .data import Data, DataValue, build_data_type
from .value import (
    BooleanValue,
    FloatValue,
    IntegerValue,
    NullValue,
    SequenceValue,
    StringMapValue,
    StringValue,
    ValueType,
    _value_registry,
)


class BaseJSONSchema(ABC, BaseModel):
    """
    https://json-schema.org/understanding-json-schema/reference/schema
    """

    model_config = ConfigDict(extra="allow", frozen=True)

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
            assert self.minimum <= self.maximum
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
            assert self.minimum <= self.maximum
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
            assert self.minLength <= self.maxLength
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
    items: JSONSchema
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
    additionalProperties: JSONSchema

    @property
    @override
    def value_type(self) -> ValueType:
        return StringMapValue[self.additionalProperties.value_type]


class ObjectJSONSchema(BaseJSONSchema):
    """
    https://json-schema.org/understanding-json-schema/reference/object
    """

    type: Literal["object"]
    properties: Mapping[str, JSONSchema]
    required: Set[str] = Field(default_factory=set)
    additionalProperties: bool = False

    @cached_property
    def data_type(self) -> Type[Data]:
        return build_data_type(
            "ObjectData",
            {k: (v.value_type, k in self.required) for k, v in self.properties.items()},
        )

    @cached_property
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

    @property
    @override
    def value_type(self) -> ValueType:
        return _value_registry.get(self.ref)


class JSONSchema(
    RootModel[
        BooleanJSONSchema
        | IntegerJSONSchema
        | JSONSchemaRef
        | NullJSONSchema
        | NumberJSONSchema
        | ObjectJSONSchema
        | SequenceJSONSchema
        | StringJSONSchema
        | StringMapJSONSchema
    ]
):
    """
    https://json-schema.org/understanding-json-schema/reference/schema
    """

    @cached_property
    def value_type(self) -> ValueType:
        return self.root.value_type


__all__ = [
    "JSONSchema",
]
