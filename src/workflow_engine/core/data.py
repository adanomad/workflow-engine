# workflow_engine/core/data.py
from collections.abc import Mapping
import json
from typing import Any, TypeAlias, TypeVar

from pydantic import BaseModel, ConfigDict, create_model

from .value import Value, ValueType


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def __init_subclass__(cls, **kwargs):
        """Ensure all fields in subclasses are Value types."""
        super().__init_subclass__(**kwargs)

        for field_name, field_info in cls.model_fields.items():
            if not issubclass(field_info.annotation, Value):  # type: ignore
                raise TypeError(
                    f"Field '{field_name}' in {cls.__name__} must be a Value type, got {field_info.annotation}"
                )

    def to_dict(self) -> Mapping[str, Value]:
        data: dict[str, Value] = {}
        for key in self.__class__.model_fields.keys():
            value = getattr(self, key)
            assert isinstance(value, Value)
            data[key] = value
        return data


DataMapping: TypeAlias = Mapping[str, Value]


def dump_data_mapping(data: DataMapping) -> Mapping[str, Any]:
    return {k: v.model_dump() for k, v in data.items()}


def serialize_data_mapping(data: DataMapping) -> str:
    return json.dumps(dump_data_mapping(data))


Input_contra = TypeVar("Input_contra", bound=Data, contravariant=True)
Output_co = TypeVar("Output_co", bound=Data, covariant=True)


def get_data_fields(cls: type[Data]) -> Mapping[str, tuple[ValueType, bool]]:
    """
    Extract the fields of a Data subclass.

    Args:
        cls: The Data subclass to extract fields from

    Returns:
        A mapping of field names to (ValueType, is_required) tuples
    """
    fields: Mapping[str, tuple[ValueType, bool]] = {}
    for k, v in cls.model_fields.items():
        assert v.annotation is not None
        assert issubclass(v.annotation, Value)
        fields[k] = (v.annotation, v.is_required())
    return fields


def build_data_type(
    name: str,
    fields: Mapping[str, tuple[ValueType, bool]],
) -> type[Data]:
    """
    Create a Data subclass whose fields are given by a mapping of field names to
    (ValueType, is_required) tuples.

    This is the inverse of get_fields() - it constructs a class that would return
    the same mapping when passed to get_fields().

    Args:
        name: The name of the class to create
        fields: Mapping of field names to (ValueType, required) tuples
        base_class: The base class to inherit from (defaults to Data)

    Returns:
        A new Pydantic BaseModel class with the specified fields
    """
    # Create field annotations dictionary
    annotations: dict[str, ValueType | tuple[ValueType, Any]] = {
        field_name: value_type if required else (value_type, None)
        for field_name, (value_type, required) in fields.items()
    }

    # Create the class dynamically
    cls = create_model(name, __base__=Data, **annotations)  # type: ignore

    return cls


__all__ = [
    "Data",
    "DataMapping",
    "Input_contra",
    "Output_co",
    "build_data_type",
    "get_data_fields",
    "dump_data_mapping",
    "serialize_data_mapping",
]
