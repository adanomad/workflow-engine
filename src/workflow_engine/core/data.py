# workflow_engine/core/data.py
from collections.abc import Mapping
import json
from typing import Any, TypeAlias, TypeVar

from pydantic import BaseModel, ConfigDict

from .value import Value


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


__all__ = [
    "Data",
    "DataMapping",
    "Input_contra",
    "Output_co",
    "dump_data_mapping",
    "serialize_data_mapping",
]
