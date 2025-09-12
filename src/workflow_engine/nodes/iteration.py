# workflow_engine/nodes/iteration.py
"""
Nodes that iterate over a sequence of items.
"""

from typing import ClassVar, Literal, Self, Type

from overrides import override

from ..core import (
    Context,
    DataValue,
    Edge,
    InputEdge,
    Node,
    NodeTypeInfo,
    OutputEdge,
    Params,
    Workflow,
    WorkflowValue,
)
from .data import (
    ExpandDataNode,
    ExpandSequenceNode,
    GatherDataNode,
    GatherSequenceNode,
    SequenceData,
)


class ForEachParams(Params):
    workflow: WorkflowValue


class ForEachNode(Node[SequenceData, SequenceData, ForEachParams]):
    """
    A node that executes the internal workflow W for each item in the input
    sequence.

    For each item i in the input sequence, create a copy of W, call it W[i].
    We expand the sequence into its individual data objects and expand
    sequence[i] into the input fields of W[i].
    Then, we gather the output of each W[i] into a single object, before
    gathering them further into a single sequence.

    The output of this node is a sequence of the same length as the input
    sequence, with each item being the output of the internal workflow.
    """

    TYPE_INFO: ClassVar[NodeTypeInfo] = NodeTypeInfo.from_parameter_type(
        name="ForEach",
        display_name="ForEach",
        description="Executes the internal workflow for each item in the input sequence.",
        version="0.4.0",
        parameter_type=ForEachParams,
    )

    type: Literal["ForEach"] = "ForEach"  # pyright: ignore[reportIncompatibleVariableOverride]

    @property
    def workflow(self) -> Workflow:
        return self.params.workflow.root

    @property
    @override
    def input_type(self) -> Type[SequenceData]:
        return SequenceData[DataValue[self.workflow.input_type]]

    @property
    @override
    def output_type(self) -> Type[SequenceData]:
        return SequenceData[DataValue[self.workflow.output_type]]

    @override
    async def run(self, context: Context, input: SequenceData) -> Workflow:
        N = len(input.sequence)

        nodes: list[Node] = []
        edges: list[Edge] = []

        expand = ExpandSequenceNode.from_length(
            id="expand",
            length=N,
            element_type=DataValue[self.workflow.input_type],
        )
        gather = GatherSequenceNode.from_length(
            id="gather",
            length=N,
            element_type=DataValue[self.workflow.output_type],
        )
        nodes.append(expand)
        nodes.append(gather)

        for i in range(N):
            namespace = f"element_{i}"
            input_adapter = ExpandDataNode.from_data_type(
                id="input_adapter",
                data_type=self.workflow.input_type,
            ).with_namespace(namespace)
            item_workflow = self.workflow.with_namespace(namespace)
            output_adapter = GatherDataNode.from_data_type(
                id="output_adapter",
                data_type=self.workflow.output_type,
            ).with_namespace(namespace)

            nodes.append(input_adapter)
            nodes.extend(item_workflow.nodes)
            nodes.append(output_adapter)

            edges.append(
                Edge.from_nodes(
                    source=expand,
                    source_key=expand.key(i),
                    target=input_adapter,
                    target_key="data",
                )
            )
            for input_edge in item_workflow.input_edges:
                edges.append(
                    Edge(
                        source_id=input_adapter.id,
                        source_key=input_edge.input_key,
                        target_id=input_edge.target_id,
                        target_key=input_edge.target_key,
                    )
                )
            edges.extend(item_workflow.edges)
            for output_edge in item_workflow.output_edges:
                edges.append(
                    Edge(
                        source_id=output_edge.source_id,
                        source_key=output_edge.source_key,
                        target_id=output_adapter.id,
                        target_key=output_edge.output_key,
                    )
                )
            edges.append(
                Edge.from_nodes(
                    source=output_adapter,
                    source_key="data",
                    target=gather,
                    target_key=gather.key(i),
                )
            )

        return Workflow(
            nodes=nodes,
            edges=edges,
            input_edges=[
                InputEdge.from_node(
                    input_key="sequence",
                    target=expand,
                    target_key="sequence",
                )
            ],
            output_edges=[
                OutputEdge.from_node(
                    source=gather,
                    source_key="sequence",
                    output_key="sequence",
                )
            ],
        )

    @classmethod
    def from_workflow(
        cls,
        id: str,
        workflow: Workflow,
    ) -> Self:
        return cls(id=id, params=ForEachParams(workflow=WorkflowValue(workflow)))


__all__ = [
    "ForEachNode",
]
