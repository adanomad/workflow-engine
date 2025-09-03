# workflow_engine/nodes/error.py

from typing import ClassVar, Literal

from ..core import (
    Context,
    Data,
    Empty,
    Node,
    NodeTypeInfo,
    Params,
    StringValue,
    UserException,
)


class ErrorInput(Data):
    info: StringValue


class ErrorParams(Params):
    error_name: StringValue


class ErrorNode(Node[ErrorInput, Empty, ErrorParams]):
    """
    A node that always raises an error.
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="Error",
        display_name="Error",
        description="A node that always raises an error.",
        version="0.4.0",
    )

    type: Literal["Error"] = "Error"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    def input_type(self):
        return ErrorInput

    async def run(self, context: Context, input: ErrorInput) -> Empty:
        raise UserException(f"{self.params.error_name}: {input.info}")

    @classmethod
    def from_name(cls, id: str, name: str) -> "ErrorNode":
        return cls(id=id, params=ErrorParams(error_name=StringValue(name)))


__all__ = [
    "ErrorNode",
]
