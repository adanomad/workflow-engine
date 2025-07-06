# workflow_engine/nodes/arithmetic.py
"""
Simple nodes for testing the workflow engine, with limited usefulness otherwise.
"""

from typing import Literal

from ..core import (
    Context,
    Data,
    Empty,
    FloatValue,
    IntegerValue,
    Node,
    Params,
    SequenceValue,
)


class AddNodeInput(Data):
    a: FloatValue
    b: FloatValue


class SumOutput(Data):
    sum: FloatValue


class AddNode(Node[AddNodeInput, SumOutput, Empty]):
    type: Literal["Add"] = "Add"

    @property
    def input_type(self):
        return AddNodeInput

    @property
    def output_type(self):
        return SumOutput

    async def run(self, context: Context, input: AddNodeInput) -> SumOutput:
        return SumOutput(sum=FloatValue(input.a.root + input.b.root))


class SumNodeInput(Data):
    values: SequenceValue[FloatValue]


class SumNodeOutput(Data):
    sum: FloatValue


class SumNode(Node[SumNodeInput, SumNodeOutput, Empty]):
    type: Literal["Sum"] = "Sum"

    @property
    def input_type(self):
        return SumNodeInput

    @property
    def output_type(self):
        return SumNodeOutput

    async def run(self, context: Context, input: SumNodeInput) -> SumNodeOutput:
        return SumNodeOutput(sum=FloatValue(sum(v.root for v in input.values.root)))


class IntegerData(Data):
    value: IntegerValue


class FactorizationData(Data):
    factors: SequenceValue[IntegerValue]


class FactorizationNode(Node[IntegerData, FactorizationData, Params]):
    type: Literal["Factorization"] = "Factorization"

    @property
    def input_type(self):
        return IntegerData

    @property
    def output_type(self):
        return FactorizationData

    async def run(self, context: Context, input: IntegerData) -> FactorizationData:
        value = input.value.root
        if value > 0:
            return FactorizationData(
                factors=SequenceValue(
                    root=[
                        IntegerValue(i) for i in range(1, value + 1) if value % i == 0
                    ]
                )
            )
        raise ValueError("Can only factorize positive integers")


__all__ = [
    "AddNode",
    "FactorizationNode",
    "SumNode",
]
