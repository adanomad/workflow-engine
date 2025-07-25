# workflow_engine/nodes/constant.py
from typing import Literal, Type

from ..core import (
    BooleanValue,
    Context,
    Data,
    Empty,
    IntegerValue,
    Node,
    Params,
    StringValue,
)


class ConstantBoolean(Params):
    value: BooleanValue


class ConstantBooleanNode(Node[Empty, ConstantBoolean, ConstantBoolean]):
    type: Literal["ConstantBoolean"] = "ConstantBoolean"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    def output_type(self) -> Type[ConstantBoolean]:
        return ConstantBoolean

    async def run(self, context: Context, input: Empty) -> ConstantBoolean:
        return self.params

    @classmethod
    def from_value(cls, id: str, value: bool) -> "ConstantBooleanNode":
        return cls(id=id, params=ConstantBoolean(value=BooleanValue(value)))


class ConstantInteger(Params):
    value: IntegerValue


class ConstantIntegerNode(Node[Empty, ConstantInteger, ConstantInteger]):
    type: Literal["ConstantInteger"] = "ConstantInteger"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    def output_type(self) -> Type[ConstantInteger]:
        return ConstantInteger

    async def run(self, context: Context, input: Empty) -> ConstantInteger:
        return self.params

    @classmethod
    def from_value(cls, id: str, value: int) -> "ConstantIntegerNode":
        return cls(id=id, params=ConstantInteger(value=IntegerValue(value)))


class ConstantString(Params):
    value: StringValue


class ConstantStringNode(Node[Empty, ConstantString, ConstantString]):
    type: Literal["ConstantString"] = "ConstantString"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    def output_type(self) -> Type[ConstantString]:
        return ConstantString

    async def run(self, context: Context, input: Data) -> ConstantString:
        return self.params

    @classmethod
    def from_value(cls, id: str, value: str) -> "ConstantStringNode":
        return cls(id=id, params=ConstantString(value=StringValue(value)))


__all__ = [
    "ConstantBooleanNode",
    "ConstantIntegerNode",
    "ConstantStringNode",
]
