# workflow_engine/nodes/constant.py
from collections.abc import Sequence
from typing import Literal, Self, Type

from overrides import override

from workflow_engine.core.data import build_data_type
from workflow_engine.core.value import StringMapValue

from ..core import (
    Context,
    Data,
    Node,
    Params,
    SequenceValue,
    StringValue,
    Value,
)


class BuildMappingParams(Params):
    keys: SequenceValue[StringValue]


class BuildMappingOutput(Data):
    mapping: StringMapValue[Value]


class BuildMappingNode(Node[Data, BuildMappingOutput, BuildMappingParams]):
    """
    Creates a new mapping object from the inputs to this node.

    Example:
        >>> node = BuildMappingNode.from_keys("node_id", ["a", "b", "c"])
        >>> node.run(context, input={"a": 1, "b": 2, "c": 3}).model_dump()
        {"mapping": {"a": 1, "b": 2, "c": 3}}
    """

    type: Literal["BuildMapping"] = "BuildMapping"

    @property
    @override
    def input_type(self) -> Type[Data]:
        return build_data_type(
            "BuildMappingInput",
            {key.root: (Value, True) for key in self.params.keys.root},
        )

    @property
    @override
    def output_type(self) -> Type[BuildMappingOutput]:
        return BuildMappingOutput

    @override
    async def run(self, context: Context, input: Data) -> BuildMappingOutput:
        return BuildMappingOutput(
            mapping=StringMapValue(
                {key.root: getattr(input, key.root) for key in self.params.keys.root}
            )
        )

    @classmethod
    def from_keys(
        cls,
        node_id: str,
        keys: Sequence[str],
    ) -> Self:
        return cls(
            id=node_id,
            params=BuildMappingParams(
                keys=SequenceValue(root=[StringValue(key) for key in keys])
            ),
        )


class ExtractKeyInput(Data):
    mapping: StringMapValue[Value]


class ExtractKeyParams(Params):
    key: StringValue


class ExtractKeyOutput(Data):
    value: Value


class ExtractKeyNode(Node[ExtractKeyInput, ExtractKeyOutput, ExtractKeyParams]):
    """
    Extracts a value from a mapping object at a specific key.

    Example:
        >>> node = ExtractKeyNode.from_key("node_id", "a")
        >>> node.run(context, input={"mapping": {"a": 1, "b": 2, "c": 3}}).model_dump()
        {"value": 1}
    """

    type: Literal["ExtractKey"] = "ExtractKey"

    @property
    @override
    def input_type(self) -> Type[ExtractKeyInput]:
        return ExtractKeyInput

    @property
    @override
    def output_type(self) -> Type[ExtractKeyOutput]:
        return ExtractKeyOutput

    @override
    async def run(self, context: Context, input: ExtractKeyInput) -> ExtractKeyOutput:
        return ExtractKeyOutput(value=input.mapping.root[self.params.key.root])

    @classmethod
    def from_key(cls, node_id: str, key: str) -> Self:
        return cls(
            id=node_id,
            params=ExtractKeyParams(key=StringValue(key)),
        )


__all__ = [
    "BuildMappingNode",
    "ExtractKeyNode",
]
