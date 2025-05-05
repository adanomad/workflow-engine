# workflow_engine/core/edge.py

from pydantic import BaseModel, ConfigDict

from ..utils.assign import is_assignable
from .node import Node


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
        target_input_type, _ = target.input_fields[self.target_key]

        # NOTE: since we're dealing with immutable data rather than functions,
        # covariance is almost always what we want here
        if not is_assignable(
            source_output_type,
            target_input_type,
            covariant=True,
        ):
            raise TypeError(
                f"Edge from {source.id}.{self.source_key} to {target.id}.{self.target_key} has invalid types: {source_output_type} is not assignable to {target_input_type}"
            )


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

    def validate_types(self, input_type: type, target: Node):
        if self.target_key not in target.input_fields:
            raise ValueError(
                f"Target node {target.id} does not have a {self.target_key} field"
            )

        target_input_type, _ = target.input_fields[self.target_key]

        # NOTE: since we're dealing with immutable data rather than functions,
        # covariance is almost always what we want here
        if not is_assignable(
            input_type,
            target_input_type,
            covariant=True,
        ):
            raise TypeError(
                f"Input edge to {target.id}.{self.target_key} has invalid types: {input_type} is not assignable to {target_input_type}"
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

    def validate_types(self, source: Node, output_type: type):
        if self.source_key not in source.output_fields:
            raise ValueError(
                f"Source node {source.id} does not have a {self.source_key} field"
            )

        source_output_type, _ = source.output_fields[self.source_key]

        # NOTE: since we're dealing with immutable data rather than functions,
        # covariance is almost always what we want here
        if not is_assignable(
            source_output_type,
            output_type,
            covariant=True,
        ):
            raise TypeError(
                f"Output edge from {source.id}.{self.source_key} has invalid types: {source_output_type} is not assignable to {output_type}"
            )


__all__ = [
    "Edge",
    "InputEdge",
    "OutputEdge",
]
