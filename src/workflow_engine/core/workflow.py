# workflow_engine/core/workflow.py
from collections.abc import Mapping, Sequence
from functools import cached_property
from itertools import chain

import networkx as nx
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from .data import DataMapping
from .edge import Edge, InputEdge, OutputEdge
from .error import UserException
from .node import Node
from .value import ValueType


class Workflow(BaseModel):
    model_config = ConfigDict(frozen=True)

    nodes: Sequence[Node]
    edges: Sequence[Edge]
    input_edges: Sequence[InputEdge]
    output_edges: Sequence[OutputEdge]

    @cached_property
    def nodes_by_id(self) -> Mapping[str, Node]:
        nodes_by_id: dict[str, Node] = {}
        for node in self.nodes:
            if node.id in nodes_by_id:
                raise ValueError(f"Node {node.id} is already in the graph")
            nodes_by_id[node.id] = node
        return nodes_by_id

    @cached_property
    def edges_by_target(self) -> Mapping[str, Mapping[str, Edge | InputEdge]]:
        """
        A mapping from each node and input key to the (unique) edge that targets
        the node at that key.
        """
        edges_by_target: dict[str, dict[str, Edge | InputEdge]] = {
            node.id: {} for node in self.nodes
        }
        for edge in chain(self.edges, self.input_edges):
            if edge.target_key in edges_by_target[edge.target_id]:
                raise ValueError(
                    f"In-edge to {edge.target_id}.{edge.target_key} is already in the graph"
                )
            edges_by_target[edge.target_id][edge.target_key] = edge
        return edges_by_target

    @cached_property
    def input_fields(self) -> Mapping[str, ValueType]:
        return {
            edge.source_key: self.nodes_by_id[edge.source_id].output_fields[
                edge.source_key
            ][0]
            for edge in self.edges
        }

    @cached_property
    def output_fields(self) -> Mapping[str, ValueType]:
        return {
            edge.target_key: self.nodes_by_id[edge.target_id].input_fields[
                edge.target_key
            ][0]
            for edge in self.edges
        }

    @cached_property
    def nx_graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()

        for node in self.nodes:
            graph.add_node(node.id, data=node)

        for edge in self.edges:
            graph.add_edge(edge.source_id, edge.target_id, data=edge)

        return graph

    @model_validator(mode="after")
    def _validate_nodes(self):
        # make sure that for each node, all input edges are present
        for node in self.nodes:
            for key, (_, required) in node.input_fields.items():
                if required and key not in self.edges_by_target[node.id]:
                    raise ValueError(f"Node {node.id} has no required input edge {key}")
        return self

    @model_validator(mode="after")
    def _validate_edges(self):
        for edge in self.edges:
            edge.validate_types(
                source=self.nodes_by_id[edge.source_id],
                target=self.nodes_by_id[edge.target_id],
            )
        return self

    @model_validator(mode="after")
    def _validate_dag(self):
        if not nx.is_directed_acyclic_graph(self.nx_graph):
            cycles = list(nx.simple_cycles(self.nx_graph))
            raise ValueError(f"Workflow graph is not a DAG. Cycles found: {cycles}")
        return self

    def get_ready_nodes(
        self,
        input: DataMapping,
        node_outputs: Mapping[str, DataMapping] | None = None,
        partial_results: Mapping[str, DataMapping] | None = None,
    ) -> Mapping[str, DataMapping]:
        """
        Given the input and the set of nodes which have already finished, return
        the nodes that are now ready to be executed and their arguments.

        Should only return an empty map if the entire workflow is finished.

        For efficiency, this method can use partial results to avoid
        recalculating already finished nodes.
        """
        if node_outputs is None:
            node_outputs = {}

        ready_nodes: dict[str, DataMapping] = (
            {} if partial_results is None else dict(partial_results)
        )
        for node in self.nodes:
            # remove the node if it is now finished
            if node.id in node_outputs:
                if node.id in ready_nodes:
                    ready_nodes.pop(node.id)
                continue
            # skip the node if it is already in the ready set
            if node.id in ready_nodes:
                continue

            # node might be ready, we have to check all its input edges
            ready: bool = True
            node_input_dict: DataMapping = {}
            for target_key, edge in self.edges_by_target[node.id].items():
                # if the input is missing, we will let the node figure it out
                if isinstance(edge, InputEdge):
                    node_input_dict[target_key] = input[edge.input_key]
                elif edge.source_id in node_outputs:
                    node_input_dict[target_key] = node_outputs[edge.source_id][
                        edge.source_key
                    ]
                else:
                    ready = False
                    break
            if not ready:
                continue

            try:
                ready_nodes[node.id] = node_input_dict
            except ValidationError as e:
                raise UserException(
                    f"Input {node_input_dict} for node {node.id} is invalid: {e}",
                )
        return ready_nodes

    def get_output(
        self,
        node_outputs: Mapping[str, DataMapping],
        partial: bool = False,
    ) -> DataMapping:
        """
        Get the output of the workflow.

        If partial is True, this method should never raise an exception, and the
        output will only include nodes that have been executed, for which the
        output field is available.
        """
        output: DataMapping = {}
        for edge in self.output_edges:
            if edge.source_id not in node_outputs:
                if partial:
                    continue
                raise UserException(
                    f"Cannot get output from node {edge.source_id}.",
                )
            node_output = node_outputs[edge.source_id]
            if edge.source_key not in node_output:
                if partial:
                    continue
                raise UserException(
                    f"Cannot get output from node {edge.source_id} at key '{edge.source_key}'.",
                )
            output_field = node_output[edge.source_key]
            output[edge.output_key] = output_field
        return output


__all__ = [
    "Workflow",
]
