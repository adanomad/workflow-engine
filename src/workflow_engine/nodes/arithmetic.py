# workflow_engine/nodes/arithmetic.py
"""
Simple nodes for testing the workflow engine, with limited usefulness otherwise.
"""

from collections.abc import Sequence
from typing import Literal

from ..core import Context, Data, Empty, Node, Params


class AddNodeInput(Data):
    a: int
    b: int


class SumOutput(Data):
    sum: int


class AddNode(Node[AddNodeInput, SumOutput, Empty]):
    type: Literal["Add"] = "Add"

    @property
    def input_type(self):
        return AddNodeInput

    @property
    def output_type(self):
        return SumOutput

    async def run(self, context: Context, input: AddNodeInput) -> SumOutput:
        return SumOutput(sum=input.a + input.b)


class SumNodeInput(Data):
    values: Sequence[int]


class SumNodeOutput(Data):
    sum: int


class SumNode(Node[SumNodeInput, SumNodeOutput, Empty]):
    type: Literal["Sum"] = "Sum"

    @property
    def input_type(self):
        return SumNodeInput

    @property
    def output_type(self):
        return SumNodeOutput

    async def run(self, context: Context, input: SumNodeInput) -> SumNodeOutput:
        return SumNodeOutput(sum=sum(input.values))


class IntData(Data):
    value: int


class FactorizationData(Data):
    factors: Sequence[int]


class FactorizationNode(Node[IntData, FactorizationData, Params]):
    type: Literal["Factorization"] = "Factorization"

    @property
    def input_type(self):
        return IntData

    @property
    def output_type(self):
        return FactorizationData

    async def run(self, context: Context, input: IntData) -> FactorizationData:
        if input.value > 0:
            return FactorizationData(
                factors=[i for i in range(1, input.value + 1) if input.value % i == 0]
            )
        raise ValueError("Can only factorize positive integers")


__all__ = [
    "AddNode",
    "FactorizationNode",
    "SumNode",
]
