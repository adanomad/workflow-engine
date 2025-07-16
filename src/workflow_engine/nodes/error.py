# workflow_engine/nodes/error.py

from typing import Literal

from ..core import (
    Context,
    Data,
    Empty,
    Node,
    StringValue,
    Params,
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

    type: Literal["Error"] = "Error"

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
