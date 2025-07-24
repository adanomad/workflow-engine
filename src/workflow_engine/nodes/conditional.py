# workflow_engine/nodes/conditional.py
"""
Conditional nodes that run different workflows depending on a condition input.
"""

from typing import Literal, Self, Type

from overrides import override
from pydantic import ConfigDict

from workflow_engine.core.data import build_data_type, get_data_fields

from ..core import (
    BooleanValue,
    Context,
    Data,
    Empty,
    Node,
    Params,
    Workflow,
)
from ..utils.mappings import mapping_intersection


class IfParams(Params):
    if_true: Workflow


class IfElseParams(Params):
    if_true: Workflow
    if_false: Workflow


class ConditionalInput(Data):
    model_config = ConfigDict(extra="allow")

    condition: BooleanValue


class IfNode(Node[ConditionalInput, Empty, IfParams]):
    """
    A node that optionally executes the internal workflow if the boolean
    condition is true.

    The output of this node is always empty, since there would be no valid
    output if the condition is false.
    """

    # TODO: allow conditional nodes with optional output

    type: Literal["If"] = "If"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    @override
    def input_type(self) -> Type[ConditionalInput]:
        fields = dict(get_data_fields(ConditionalInput))
        for key, field in self.params.if_true.input_fields.items():
            assert key not in fields
            fields[key] = field
        return build_data_type("IfInput", fields, base_cls=ConditionalInput)

    @property
    @override
    def output_type(self) -> Type[Empty]:
        return Empty

    @override
    async def run(self, context: Context, input: ConditionalInput) -> Empty | Workflow:
        if input.condition:
            return self.params.if_true
        return Empty()

    @classmethod
    def from_workflow(
        cls,
        id: str,
        if_true: Workflow,
    ) -> Self:
        return cls(id=id, params=IfParams(if_true=if_true))


class IfElseNode(Node[ConditionalInput, Data, IfElseParams]):
    """
    A node that executes one of the two internal workflows based on the boolean
    condition.

    The output of this node is the intersection of the if_true and if_false
    workflows.
    """

    # TODO: allow union types

    type: Literal["IfElse"] = "IfElse"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    @override
    def input_type(self) -> Type[ConditionalInput]:
        fields = dict(get_data_fields(ConditionalInput))
        for key, field in self.params.if_true.input_fields.items():
            assert key not in fields
            fields[key] = field
        return build_data_type("IfElseInput", fields, base_cls=ConditionalInput)

    @property
    @override
    def output_type(self) -> Type[Data]:
        fields = mapping_intersection(
            self.params.if_true.output_fields,
            self.params.if_false.output_fields,
        )
        return build_data_type("IfElseOutput", fields)

    @override
    async def run(self, context: Context, input: ConditionalInput) -> Workflow:
        return self.params.if_true if input.condition else self.params.if_false

    @classmethod
    def from_workflows(
        cls,
        id: str,
        if_true: Workflow,
        if_false: Workflow,
    ) -> Self:
        return cls(
            id=id,
            params=IfElseParams(
                if_true=if_true,
                if_false=if_false,
            ),
        )


__all__ = [
    "ConditionalInput",
    "IfNode",
    "IfElseNode",
]
