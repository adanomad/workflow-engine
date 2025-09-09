# workflow_engine/nodes/data.py

"""
Utility nodes to construct and deconstruct data objects.
"""

from collections.abc import Sequence
from typing import ClassVar, Generic, Literal, Self, Type, TypeVar

from overrides import override
from pydantic import Field

from workflow_engine.core.data import build_data_type

from ..core import (
    Context,
    Data,
    DataValue,
    Empty,
    IntegerValue,
    Node,
    NodeTypeInfo,
    Params,
    SequenceValue,
    StringMapValue,
    StringValue,
    Value,
    ValueType,
)

V = TypeVar("V", bound=Value)


################################################################################
# Sequences


class SequenceParams(Params):
    length: IntegerValue


class SequenceData(Data, Generic[V]):
    sequence: SequenceValue[V]


class GatherSequenceNode(Node[Data, SequenceData, SequenceParams]):
    """
    Creates a new sequence object of a given length.

    Example:
        >>> node = GatherSequenceNode.from_length("node_id", 3)
        >>> node.run(context, input={}).model_dump()
        {"sequence": [0, 1, 2]}
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="GatherSequence",
        display_name="GatherSequence",
        description="Creates a new sequence object of a given length.",
        version="0.4.0",
    )

    type: Literal["GatherSequence"] = "GatherSequence"  # pyright: ignore[reportIncompatibleVariableOverride]

    # The type of the elements in the sequence.
    # For now, this field is only available when the node is constructed
    # programmatically.
    # TODO: make this available at runtime
    element_type: ValueType = Field(default=Value, exclude=True)

    def key(self, index: int) -> str:
        return f"element_{index}"

    @property
    def keys(self) -> Sequence[str]:
        N = self.params.length.root
        return [self.key(i) for i in range(N)]

    @property
    @override
    def input_type(self) -> Type[Data]:
        return build_data_type(
            "GatherSequenceInput",
            {key: (self.element_type, True) for key in self.keys},
        )

    @property
    @override
    def output_type(self) -> Type[SequenceData]:
        return SequenceData[self.element_type]

    @override
    async def run(self, context: Context, input: Data) -> SequenceData:
        input_dict = input.to_dict()
        return self.output_type(
            sequence=SequenceValue[self.element_type](
                root=[input_dict[key] for key in self.keys]
            )
        )

    @classmethod
    def from_length(
        cls,
        id: str,
        length: int,
        element_type: ValueType = Value,
    ) -> Self:
        return cls(
            id=id,
            params=SequenceParams(length=IntegerValue(root=length)),
            element_type=element_type,
        )


class ExpandSequenceNode(Node[SequenceData, Data, SequenceParams]):
    """
    Extracts a sequence of elements to a data object.
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="ExpandSequence",
        display_name="ExpandSequence",
        description="Extracts a sequence of elements to a data object.",
        version="0.4.0",
    )

    type: Literal["ExpandSequence"] = "ExpandSequence"  # pyright: ignore[reportIncompatibleVariableOverride]

    # The type of the element to extract.
    # For now, this field is only available when the node is constructed
    # programmatically.
    # TODO: make this available at runtime
    element_type: ValueType = Value

    def key(self, index: int) -> str:
        return f"element_{index}"

    @property
    def keys(self) -> Sequence[str]:
        N = self.params.length.root
        return [self.key(i) for i in range(N)]

    @property
    @override
    def input_type(self) -> Type[SequenceData]:
        return SequenceData[self.element_type]

    @property
    @override
    def output_type(self) -> Type[Data]:
        return build_data_type(
            "ExpandSequenceOutput",
            {key: (self.element_type, True) for key in self.keys},
        )

    @override
    async def run(
        self,
        context: Context,
        input: SequenceData,
    ) -> Data:
        N = self.params.length.root
        assert len(input.sequence) == N, (
            f"Expected sequence of length {N}, but got {len(input.sequence)}"
        )
        return self.output_type(**{self.key(i): input.sequence[i] for i in range(N)})

    @classmethod
    def from_length(
        cls,
        id: str,
        length: int,
        element_type: ValueType = Value,
    ) -> Self:
        return cls(
            id=id,
            params=SequenceParams(length=IntegerValue(root=length)),
            element_type=element_type,
        )


################################################################################
# Mappings


class MappingParams(Params):
    keys: SequenceValue[StringValue]


class MappingData(Data, Generic[V]):
    mapping: StringMapValue[V]


class GatherMappingNode(Node[Data, MappingData, MappingParams]):
    """
    Creates a new mapping object from the inputs to this node.

    Example:
        >>> node = GatherMappingNode.from_keys("node_id", ["a", "b", "c"])
        >>> node.run(context, input={"a": 1, "b": 2, "c": 3}).model_dump()
        {"mapping": {"a": 1, "b": 2, "c": 3}}
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="GatherMapping",
        display_name="GatherMapping",
        description="Creates a new mapping object from the inputs to this node.",
        version="0.4.0",
    )

    type: Literal["GatherMapping"] = "GatherMapping"  # pyright: ignore[reportIncompatibleVariableOverride]

    # The type of the values in the mapping.
    # For now, this field is only available when the node is constructed
    # programmatically.
    # TODO: make this serializable/deserializable
    value_type: ValueType = Value

    @property
    @override
    def input_type(self) -> Type[Data]:
        return build_data_type(
            "GatherMappingInput",
            {key.root: (self.value_type, True) for key in self.params.keys},
        )

    @property
    @override
    def output_type(self) -> Type[MappingData]:
        return MappingData[self.value_type]

    @override
    async def run(self, context: Context, input: Data) -> MappingData:
        return self.output_type(
            mapping=StringMapValue[self.value_type](
                {key.root: getattr(input, key.root) for key in self.params.keys}
            )
        )

    @classmethod
    def from_keys(
        cls,
        id: str,
        keys: Sequence[str],
    ) -> Self:
        return cls(
            id=id,
            params=MappingParams(
                keys=SequenceValue[StringValue]([StringValue(key) for key in keys])
            ),
        )


class ExpandMappingNode(Node[MappingData, Data, MappingParams]):
    """
    Extracts values from a mapping object at specific keys.

    Example:
        >>> node = ExpandMappingNode.from_keys("node_id", ["a", "b", "c"])
        >>> node.run(context, input={"mapping": {"a": 1, "b": 2, "c": 3}}).model_dump()
        {"a": 1, "b": 2, "c": 3}
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="ExpandMapping",
        display_name="ExpandMapping",
        description="Extracts values from a mapping object at specific keys.",
        version="0.4.0",
    )

    type: Literal["ExpandMapping"] = "ExpandMapping"  # pyright: ignore[reportIncompatibleVariableOverride]

    # The type of the values in the mapping.
    # For now, this field is only available when the node is constructed
    # programmatically.
    # TODO: make this serializable/deserializable
    value_type: ValueType = Value

    @property
    @override
    def input_type(self) -> Type[MappingData]:
        return MappingData[self.value_type]

    @property
    @override
    def output_type(self) -> Type[Data]:
        return build_data_type(
            "ExpandMappingOutput",
            {key.root: (self.value_type, True) for key in self.params.keys},
        )

    @override
    async def run(self, context: Context, input: MappingData) -> Data:
        return self.output_type(
            **{key.root: input.mapping[key] for key in self.params.keys}
        )

    @classmethod
    def from_keys(cls, id: str, keys: Sequence[str]) -> Self:
        return cls(
            id=id,
            params=MappingParams(
                keys=SequenceValue[StringValue]([StringValue(key) for key in keys])
            ),
        )


################################################################################
# Data nodes


D = TypeVar("D", bound=Data)


class NestedData(Data, Generic[D]):
    """
    A data type that contains a nested data object.
    """

    data: DataValue[D]


class GatherDataNode(Node[Data, NestedData, Empty]):
    """
    A node that gathers a data object from the inputs to this node.

    Example:
        >>> node = GatherDataNode.from_data_type("node_id", Data)
        >>> node.run(context, input={"a": 1, "b": 2, "c": 3}).model_dump()
        {"data": {"a": 1, "b": 2, "c": 3}}
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="GatherData",
        display_name="GatherData",
        description="A node that gathers a data object from the inputs to this node.",
        version="0.4.0",
    )

    type: Literal["GatherData"] = "GatherData"  # pyright: ignore[reportIncompatibleVariableOverride]

    # The type of the element to extract.
    # For now, this field is only available when the node is constructed
    # programmatically.
    # TODO: make this serializable/deserializable
    data_type: Type[Data] = Field(default=Data, exclude=True)

    @property
    @override
    def input_type(self) -> Type[Data]:
        return self.data_type

    @property
    @override
    def output_type(self) -> Type[NestedData]:
        return NestedData[self.data_type]

    @override
    async def run(self, context: Context, input: Data) -> NestedData:
        return NestedData[self.data_type](data=DataValue[self.data_type](root=input))

    @classmethod
    def from_data_type(cls, id: str, data_type: Type[Data]) -> Self:
        return cls(
            id=id,
            params=Empty(),
            data_type=data_type,
        )


class ExpandDataNode(Node[NestedData, Data, Empty]):
    """
    A node that expands a nested data object into its individual fields.

    Example:
        >>> node = ExpandDataNode.from_data_type("node_id", Data)
        >>> node.run(context, input={"data": {"a": 1, "b": 2, "c": 3}}).model_dump()
        {"a": 1, "b": 2, "c": 3}
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo(
        name="ExpandData",
        display_name="ExpandData",
        description="A node that expands a nested data object into its individual fields.",
        version="0.4.0",
    )

    type: Literal["ExpandData"] = "ExpandData"  # pyright: ignore[reportIncompatibleVariableOverride]

    # The type of the nested data object.
    # For now, this field is only available when the node is constructed
    # programmatically.
    # TODO: make this serializable/deserializable
    data_type: Type[Data] = Field(default=Data, exclude=True)

    @property
    @override
    def input_type(self) -> Type[NestedData]:
        return NestedData[self.data_type]

    @property
    @override
    def output_type(self) -> Type[Data]:
        return self.data_type

    @override
    async def run(self, context: Context, input: NestedData) -> Data:
        return input.data.root

    @classmethod
    def from_data_type(cls, id: str, data_type: Type[Data]) -> Self:
        return cls(
            id=id,
            params=Empty(),
            data_type=data_type,
        )


__all__ = [
    "ExpandDataNode",
    "ExpandMappingNode",
    "ExpandSequenceNode",
    "GatherDataNode",
    "GatherMappingNode",
    "GatherSequenceNode",
]
