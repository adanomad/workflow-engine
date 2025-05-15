# workflow_engine/nodes/constant.py
from typing import Literal, Type

from ..core import Context, Data, Empty, Node, Params


class ConstantBool(Params):
    value: bool


class ConstantBoolNode(Node[Empty, ConstantBool, ConstantBool]):
    type: Literal["ConstantBool"] = "ConstantBool"

    @property
    def output_type(self):
        return ConstantBool

    async def run(self, context: Context, input: Empty) -> ConstantBool:
        return self.params

    @classmethod
    def from_value(cls, node_id: str, value: bool) -> "ConstantBoolNode":
        return cls(id=node_id, params=ConstantBool(value=value))


class ConstantInt(Params):
    value: int


class ConstantIntNode(Node[Empty, ConstantInt, ConstantInt]):
    type: Literal["ConstantInt"] = "ConstantInt"

    @property
    def output_type(self):
        return ConstantInt

    async def run(self, context: Context, input: Empty) -> ConstantInt:
        return self.params

    @classmethod
    def from_value(cls, node_id: str, value: int) -> "ConstantIntNode":
        return cls(id=node_id, params=ConstantInt(value=value))


class ConstantString(Params):
    value: str


class ConstantStringNode(Node[Empty, ConstantString, ConstantString]):
    type: Literal["ConstantString"] = "ConstantString"

    @property
    def output_type(self) -> Type[ConstantString]:
        return ConstantString

    async def run(self, context: Context, input: Data) -> ConstantString:
        return ConstantString(value=self.params.value)

    @classmethod
    def from_value(cls, node_id: str, value: str) -> "ConstantStringNode":
        return cls(id=node_id, params=ConstantString(value=value))


__all__ = [
    "ConstantBool",
    "ConstantBoolNode",
    "ConstantInt",
    "ConstantIntNode",
    "ConstantString",
    "ConstantStringNode",
]
