# workflow_engine/nodes/arithmetic.py
"""
Simple nodes for testing the workflow engine, with limited usefulness otherwise.
"""

from typing import ClassVar, Literal

from ..core import (
    Context,
    Data,
    Empty,
    FloatValue,
    IntegerValue,
    Node,
    NodeTypeInfo,
    Params,
    SequenceValue,
)


class AddNodeInput(Data):
    a: FloatValue
    b: FloatValue


class SumOutput(Data):
    sum: FloatValue


class AddNode(Node[AddNodeInput, SumOutput, Empty]):
    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="Add",
        display_name="Add",
        description="Adds two numbers.",
        version="0.4.0",
    )

    type: Literal["Add"] = "Add"  # pyright: ignore[reportIncompatibleVariableOverride]

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
    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="Sum",
        display_name="Sum",
        description="Sums a sequence of numbers.",
        version="0.4.0",
    )

    type: Literal["Sum"] = "Sum"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    def input_type(self):
        return SumNodeInput

    @property
    def output_type(self):
        return SumNodeOutput

    async def run(self, context: Context, input: SumNodeInput) -> SumNodeOutput:
        return SumNodeOutput(sum=FloatValue(sum(v.root for v in input.values)))


class IntegerData(Data):
    value: IntegerValue


class FactorizationData(Data):
    factors: SequenceValue[IntegerValue]


class FactorizationNode(Node[IntegerData, FactorizationData, Params]):
    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="Factorization",
        display_name="Factorization",
        description="Factorizes an integer into a sequence of its factors.",
        version="0.4.0",
    )

    type: Literal["Factorization"] = "Factorization"  # pyright: ignore[reportIncompatibleVariableOverride]

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
                    [IntegerValue(i) for i in range(1, value + 1) if value % i == 0]
                )
            )
        raise ValueError("Can only factorize positive integers")


__all__ = [
    "AddNode",
    "FactorizationNode",
    "SumNode",
]
