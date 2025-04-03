# my_workflow_engine/workflow.py
import networkx as nx
import logging
import inspect
from typing import Callable, Dict, Any, List
from .types import (
    WorkflowGraph,
    WorkflowRunResults,
    File,
    Edge,
    NodeInputData,
    NodeOutputData,
    FileExecutionData,
)
from .resolvers import BaseResolver


logger = logging.getLogger(__name__)


class WorkflowExecutionError(Exception):
    """Custom exception for workflow execution errors."""

    def __init__(self, message, node_id=None, node_name=None, original_exception=None):
        self.node_id = node_id
        self.node_name = node_name
        self.original_exception = original_exception
        full_message = "Workflow Error"
        if node_id:
            node_identifier = (
                f"'{node_name}' ({node_id})" if node_name else f"'{node_id}'"
            )
            full_message += f" at Node {node_identifier}"
        full_message += f": {message}"
        if original_exception:
            full_message += f"\n  Original Exception: {type(original_exception).__name__}: {original_exception}"
        super().__init__(full_message)


class WorkflowExecutor:
    def __init__(self, resolver: BaseResolver):
        self.resolver = resolver
        self.graph = nx.DiGraph()

    def load_workflow(self, workflow_data: Dict[str, Any]):
        """Loads and validates the workflow graph."""
        try:
            workflow = WorkflowGraph.model_validate(workflow_data)
        except Exception as e:
            logger.error(f"Workflow validation failed: {e}", exc_info=True)
            raise ValueError(f"Invalid workflow definition: {e}") from e

        self.graph = nx.DiGraph()

        for node in workflow.nodes:
            self.graph.add_node(node.id, data=node)

        for edge_data in workflow.edges:
            edge = Edge.model_validate(edge_data)
            self.graph.add_edge(edge.source_node_id, edge.target_node_id, data=edge)

        if not nx.is_directed_acyclic_graph(self.graph):
            cycles = list(nx.simple_cycles(self.graph))
            logger.error(f"Workflow graph contains cycles: {cycles}")
            raise ValueError(f"Workflow graph is not a DAG. Cycles found: {cycles}")

        logger.info("Workflow loaded and validated successfully.")

    async def execute(self):
        """
        Executes the loaded workflow graph topologically.
        Returns:
            WorkflowRunResults: A dictionary mapping node IDs to their output data.
        """

        if not self.graph:
            raise WorkflowExecutionError("Workflow not loaded before execution.")

        try:
            execution_order = list(nx.topological_sort(self.graph))
            logger.info(f"Node Execution Order: {execution_order}")
        except nx.NetworkXUnfeasible:
            logger.error(
                "Cannot topologically sort the graph (potentially disconnected or other issue)."
            )
            raise WorkflowExecutionError(
                "Cannot determine execution order (graph issue)."
            )

        workflow_results: WorkflowRunResults = {}
        logger.info("--- Starting Workflow Execution ---")

        for node_id in execution_order:
            node_data = self.graph.nodes[node_id]["data"]
            node_name = node_data.name
            logger.info(f"Executing Node: {node_data.name} ({node_id})")

            node_inputs: NodeInputData = {}  # {param_name: [FileExecutionData]}

            # 1. Gather Input Files from Predecessor Edges -> List[File] per parameter
            incoming_edges = self.graph.in_edges(node_id, data=True)
            for source_id, target_id, edge_data in incoming_edges:
                edge = edge_data["data"]
                mime_type = edge.mime_type
                param_name = edge.target_parameter

                try:
                    source_files = await self.resolver.get_node_files(
                        node_id=source_id,
                        mime_type=mime_type,
                    )
                    if param_name not in node_inputs:
                        node_inputs[edge.target_parameter] = []
                    node_inputs[param_name].extend(source_files)
                except Exception as e:
                    logger.error(
                        f"Failed to get input files for edge {edge.id} from node {source_id}",
                        exc_info=True,
                    )
                    raise WorkflowExecutionError(
                        f"Failed to resolve inputs for edge {edge.id}",
                        node_id=node_id,
                        original_exception=e,
                    )

            # 2. Get Function Implementation
            function_callable = await self.resolver.get_function(node_data.reference_id)
            function_config_params = await self.resolver.get_function_config(
                node_data.id
            )
            node_inputs.update(function_config_params)

            # 3. Execute the Node Function
            node_output: NodeOutputData = await self._execute_node(
                node_id=node_id,
                node_name=node_name,
                function=function_callable,
                node_inputs=node_inputs,
            )
            logger.info(
                f"Node {node_name} executed, produced {len(node_output)} result objects."
            )

            # 4. Save Node Results
            try:
                final_saved_metadata: List[File] = (
                    await self.resolver.save_node_results(node_id, node_output)
                )
                logger.info(
                    f"Results saved for node {node_name}, {len(final_saved_metadata)} files persisted."
                )
            except Exception as e:
                logger.error(
                    f"Failed to save results for node {node_id}", exc_info=True
                )
                raise WorkflowExecutionError(
                    "Failed to save node results",
                    node_id=node_id,
                    node_name=node_name,
                    original_exception=e,
                )

            workflow_results[node_id] = final_saved_metadata

        logger.info("--- Workflow Execution Finished ---")
        return workflow_results

    async def _execute_node(
        self,
        node_id: str,
        node_name: str,
        function: Callable,
        node_inputs: NodeInputData,
    ) -> NodeOutputData:
        """
        Prepares arguments, calls the node's function, and returns its output.
        """
        sig = inspect.signature(function)
        params = sig.parameters
        call_args = {}
        provided_params = set(node_inputs.keys())
        required_params = {
            name
            for name, p in params.items()
            if p.default == inspect.Parameter.empty
            and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
        }
        function_param_names = set(params.keys())

        # Check for missing required parameters
        missing_required = required_params - provided_params
        if missing_required:
            raise WorkflowExecutionError(
                f"Missing required parameters: {missing_required}",
                node_id=node_id,
                node_name=node_name,
            )

        for name, value in node_inputs.items():
            if name in function_param_names:
                call_args[name] = value
            else:
                logger.warning(
                    f"Node {node_id} ({node_name}): Input '{name}' provided but not used by function '{function.__name__}'."
                )

        # Call function
        try:
            if inspect.iscoroutinefunction(function):
                result = await function(**call_args)
            else:
                result = function(**call_args)

            if not isinstance(result, list) or not all(
                isinstance(item, FileExecutionData) for item in result
            ):
                logger.error(
                    f"Node {node_id} function '{function.__name__}' did not return List[FileExecutionData]. Got: {type(result)}"
                )
                return []

            return result
        except Exception as e:
            logger.error(
                f"Error executing function '{function.__name__}' for node {node_id} ({node_name})",
                exc_info=True,
            )
            raise WorkflowExecutionError(
                f"Function execution failed",
                node_id=node_id,
                node_name=node_name,
                original_exception=e,
            )
