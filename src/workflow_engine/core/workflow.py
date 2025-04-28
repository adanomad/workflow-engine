# workflow_engine/core/workflow.py
from functools import cached_property
from itertools import chain
from typing import Any, Mapping, Sequence, Type

from pydantic import BaseModel, ConfigDict, model_validator
import networkx as nx

from .data import Data
from .edge import Edge, InputEdge, OutputEdge
from .node import Node


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
                raise ValueError(f"In-edge to {edge.target_id}.{edge.target_key} is already in the graph")
            edges_by_target[edge.target_id][edge.target_key] = edge
        return edges_by_target

    @cached_property
    def input_fields(self) -> Mapping[str, Type[Any]]:
        return {
            edge.source_key: self.nodes_by_id[edge.source_id].output_fields[edge.source_key][0]
            for edge in self.edges
        }

    @cached_property
    def output_fields(self) -> Mapping[str, Type[Any]]:
        return {
            edge.target_key: self.nodes_by_id[edge.target_id].input_fields[edge.target_key][0]
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
            input: Mapping[str, Any],
            node_outputs: Mapping[str, Data] | None = None,
            partial_results: Mapping[str, Data] | None = None,
    ) -> Mapping[str, Data]:
        """
        Given the input and the set of nodes which have already finished, return
        the nodes that are now ready to be executed and their arguments.

        Should only return an empty map if the entire workflow is finished.

        For efficiency, this method can use partial results to avoid
        recalculating already finished nodes.
        """
        if node_outputs is None:
            node_outputs = {}

        ready_nodes: dict[str, Data] = {} if partial_results is None else dict(partial_results)
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
            node_input_dict: dict[str, Any] = {}
            for target_key, edge in self.edges_by_target[node.id].items():
                if isinstance(edge, InputEdge):
                    node_input_dict[target_key] = input[edge.input_key]
                elif edge.source_id in node_outputs:
                    node_input_dict[target_key] = getattr(node_outputs[edge.source_id], edge.source_key)
                else:
                    ready = False
                    break
            if not ready:
                continue

            ready_nodes[node.id] = node.input_type.model_validate(node_input_dict)
        return ready_nodes

    def get_output(
            self,
            node_outputs: Mapping[str, Data],
    ) -> Mapping[str, Any]:
        output: dict[str, Any] = {}
        for edge in self.output_edges:
            output_field = getattr(node_outputs[edge.source_id], edge.source_key)
            if isinstance(output_field, BaseModel):
                output_field = output_field.model_dump()
            output[edge.output_key] = output_field
        return output


__all__ = [
    "Workflow",
]
