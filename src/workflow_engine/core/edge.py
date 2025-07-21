# workflow_engine/core/edge.py

from pydantic import BaseModel, ConfigDict

from .node import Node
from .data import DataType, get_data_field
from .value import Value


class Edge(BaseModel):
    """
    An edge connects the output of source node to the input of a target node.
    """

    model_config = ConfigDict(frozen=True)

    source_id: str
    source_key: str
    target_id: str
    target_key: str

    @classmethod
    def from_nodes(
        cls,
        *,
        source: Node,
        source_key: str,
        target: Node,
        target_key: str,
    ) -> "Edge":
        """
        Self-validating factory method.
        """
        edge = cls(
            source_id=source.id,
            source_key=source_key,
            target_id=target.id,
            target_key=target_key,
        )
        edge.validate_types(source, target)
        return edge

    def validate_types(self, source: Node, target: Node):
        if self.source_key not in source.output_fields:
            raise ValueError(
                f"Source node {source.id} does not have a {self.source_key} field"
            )

        if self.target_key not in target.input_fields:
            raise ValueError(
                f"Target node {target.id} does not have a {self.target_key} field"
            )

        source_output_type, _ = source.output_fields[self.source_key]
        assert issubclass(source_output_type, Value)
        target_input_type, _ = target.input_fields[self.target_key]
        assert issubclass(target_input_type, Value)

        if not source_output_type.can_cast_to(target_input_type):
            raise TypeError(
                f"Edge from {source.id}.{self.source_key} to {target.id}.{self.target_key} has invalid types: {source_output_type} is not assignable to {target_input_type}"
            )


class SynchronizationEdge(BaseModel):
    """
    An edge that carries no information, but indicates that the target node must
    run after the source node finishes.
    """

    model_config = ConfigDict(frozen=True)

    source_id: str
    target_id: str


class InputEdge(BaseModel):
    """
    An "edge" that maps a field from the workflow's input to the input of a
    target node.
    """

    model_config = ConfigDict(frozen=True)

    input_key: str
    target_id: str
    target_key: str

    @classmethod
    def from_node(
        cls,
        *,
        input_key: str,
        target: Node,
        target_key: str,
    ) -> "InputEdge":
        return cls(
            input_key=input_key,
            target_id=target.id,
            target_key=target_key,
        )

    def validate_types(self, input_type: DataType, target: Node):
        if self.target_key not in target.input_fields:
            raise ValueError(
                f"Target node {target.id} does not have a {self.target_key} field"
            )

        source_type, source_is_required = get_data_field(input_type, self.input_key)
        target_type, target_is_required = target.input_fields[self.target_key]

        if target_is_required and not source_is_required:
            raise ValueError(
                f"Input edge to {target.id}.{self.target_key} is required, but the input data field {self.input_key} does not require it"
            )

        if not source_type.can_cast_to(target_type):
            raise TypeError(
                f"Input edge to {target.id}.{self.target_key} has invalid types: {source_type} is not assignable to {target_type}"
            )


class OutputEdge(BaseModel):
    """
    An "edge" that maps a source node's output to a special output of the
    workflow.
    """

    model_config = ConfigDict(frozen=True)

    source_id: str
    source_key: str
    output_key: str

    @classmethod
    def from_node(
        cls,
        *,
        source: Node,
        source_key: str,
        output_key: str,
    ) -> "OutputEdge":
        return cls(
            source_id=source.id,
            source_key=source_key,
            output_key=output_key,
        )

    def validate_types(self, source: Node, output_type: DataType):
        if self.source_key not in source.output_fields:
            raise ValueError(
                f"Source node {source.id} does not have a {self.source_key} field"
            )

        source_type, source_is_required = source.output_fields[self.source_key]
        target_type, target_is_required = get_data_field(output_type, self.output_key)

        if target_is_required and not source_is_required:
            raise ValueError(
                f"Output edge from {source.id}.{self.source_key} is required, but the source node does not require it"
            )

        if not source_type.can_cast_to(target_type):
            raise TypeError(
                f"Output edge from {source.id}.{self.source_key} has invalid types: {source_type} is not assignable to {target_type}"
            )


__all__ = [
    "Edge",
    "InputEdge",
    "OutputEdge",
    "SynchronizationEdge",
]
