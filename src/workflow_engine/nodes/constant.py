from typing import Literal, Type

from ..core import Context, Data, Node


class ConstantString(Data):
    value: str

class ConstantStringNode(Node[Data, ConstantString, ConstantString]):
    type: Literal["ConstantString"] = "ConstantString"

    @property
    def output_type(self) -> Type[ConstantString]:
        return ConstantString

    def __call__(self, context: Context, input: Data) -> ConstantString:
        return self.params

    @classmethod
    def from_value(cls, node_id: str, value: str) -> "ConstantStringNode":
        return cls(id=node_id, params=ConstantString(value=value))


class ConstantInt(Data):
    value: int

class ConstantIntNode(Node[Data, ConstantInt, ConstantInt]):
    type: Literal["ConstantInt"] = "ConstantInt"

    @property
    def output_type(self) -> Type[ConstantInt]:
        return ConstantInt

    def __call__(self, context: Context, input: Data) -> ConstantInt:
        return self.params

    @classmethod
    def from_value(cls, node_id: str, value: int) -> "ConstantIntNode":
        return cls(id=node_id, params=ConstantInt(value=value))


__all__ = [
    "ConstantStringNode",
    "ConstantIntNode",
]
