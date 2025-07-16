# workflow_engine/nodes/iteration.py
"""
Nodes that iterate over a sequence of items.
"""

from typing import Literal, Self, Type

from overrides import override

from workflow_engine.core.data import DataValue
from workflow_engine.core.edge import Edge

from ..core import (
    Context,
    Data,
    InputEdge,
    OutputEdge,
    Node,
    Params,
    SequenceValue,
    StringMapValue,
    Workflow,
)
from .data import ExpandDataNode, ExpandSequenceNode, GatherDataNode, GatherSequenceNode


class ForEachParams(Params):
    workflow: Workflow


class ForEachInput(Data):
    sequence: SequenceValue[StringMapValue]


class ForEachOutput(Data):
    sequence: SequenceValue[StringMapValue]


class ForEachNode(Node[ForEachInput, ForEachOutput, ForEachParams]):
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

    type: Literal["ForEach"] = "ForEach"

    @property
    @override
    def input_type(self) -> Type[ForEachInput]:
        return ForEachInput

    @property
    @override
    def output_type(self) -> Type[ForEachOutput]:
        return ForEachOutput

    @override
    async def run(self, context: Context, input: ForEachInput) -> Workflow:
        N = len(input.sequence.root)
        workflow = self.params.workflow

        nodes: list[Node] = []
        edges: list[Edge] = []

        gather = GatherSequenceNode.from_length(
            node_id="gather",
            length=N,
            element_type=DataValue[workflow.input_type],
        )
        expand = ExpandSequenceNode.from_length(
            node_id="expand",
            length=N,
            element_type=DataValue[workflow.output_type],
        )
        nodes.append(gather)
        nodes.append(expand)

        for i in range(N):
            input_adapter = GatherDataNode.from_data_type(
                node_id=f"{i}/input_adapter",
                data_type=workflow.input_type,
            )
            item_workflow = workflow.with_namespace(str(i))
            output_adapter = ExpandDataNode.from_data_type(
                node_id=f"{i}/output_adapter",
                data_type=workflow.output_type,
            )

            nodes.append(input_adapter)
            nodes.extend(item_workflow.nodes)
            nodes.append(output_adapter)

            edges.append(
                Edge.from_nodes(
                    source=gather,
                    source_key=gather.key(i),
                    target=input_adapter,
                    target_key="data",
                )
            )
            for input_edge in item_workflow.input_edges:
                field_name = input_edge.target_key
                edges.append(
                    Edge(
                        source_id=input_adapter.id,
                        source_key=field_name,
                        target_id=input_edge.target_id,
                        target_key=field_name,
                    )
                )
            edges.extend(item_workflow.edges)
            for output_edge in item_workflow.output_edges:
                field_name = output_edge.source_key
                edges.append(
                    Edge(
                        source_id=output_edge.source_id,
                        source_key=field_name,
                        target_id=output_adapter.id,
                        target_key=field_name,
                    )
                )
            edges.append(
                Edge.from_nodes(
                    source=output_adapter,
                    source_key="data",
                    target=expand,
                    target_key=expand.key(i),
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
                    source=expand,
                    source_key="sequence",
                    output_key="sequence",
                )
            ],
        )

    @classmethod
    def from_workflow(
        cls,
        node_id: str,
        workflow: Workflow,
    ) -> Self:
        return cls(id=node_id, params=ForEachParams(workflow=workflow))


__all__ = [
    "ForEachNode",
]
