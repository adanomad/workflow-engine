# workflow_engine/nodes/error.py

from typing import Literal

from ..core import (
    Context,
    Data,
    Empty,
    Node,
    Params,
    UserException,
)


class ErrorInput(Data):
    info: str


class ErrorParams(Params):
    error_name: str


class ErrorNode(Node[ErrorInput, Empty, ErrorParams]):
    """
    A node that always raises an error.
    """

    type: Literal["Error"] = "Error"

    @property
    def input_type(self):
        return ErrorInput

    def run(self, context: Context, input: ErrorInput) -> Empty:
        raise UserException(f"{self.params.error_name}: {input.info}")

    @classmethod
    def from_name(cls, node_id: str, name: str) -> "ErrorNode":
        return cls(id=node_id, params=ErrorParams(error_name=name))


__all__ = [
    "ErrorNode",
    "ErrorParams",
]
