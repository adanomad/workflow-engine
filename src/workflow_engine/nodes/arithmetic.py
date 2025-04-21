# workflow_engine/nodes/arithmetic.py
"""
Simple nodes for testing the workflow engine, with limited usefulness otherwise.
"""

from typing import Literal, Type

from ..core import Context, Data, Node, Params


class AddNodeInput(Data):
    a: int
    b: int

class AddNodeOutput(Data):
    sum: int

class AddNode(Node[AddNodeInput, AddNodeOutput, Params]):
    type: Literal["Add"] = "Add"

    @property
    def input_type(self) -> Type[AddNodeInput]:
        return AddNodeInput

    @property
    def output_type(self) -> Type[AddNodeOutput]:
        return AddNodeOutput

    def __call__(self, context: Context, input: AddNodeInput) -> AddNodeOutput:
        return AddNodeOutput(sum=input.a + input.b)


__all__ = [
    "AddNode",
]
