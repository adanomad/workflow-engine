# workflow_engine/core/data.py
from typing import TypeVar

from pydantic import BaseModel, ConfigDict


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

Input = TypeVar("Input", bound=Data)
Input_contra = TypeVar("Input_contra", bound=Data, contravariant=True)

Output = TypeVar("Output", bound=Data)
Output_co = TypeVar("Output_co", bound=Data, covariant=True)


__all__ = [
    "Data",
    "Input",
    "Output",
]
