# workflow_engine/execution/topological.py
from typing import Any, Mapping

from overrides import override

from ..core import Context, Data, ExecutionAlgorithm,Workflow


class TopologicalExecutionAlgorithm(ExecutionAlgorithm):
    """
    Executes the workflow one node at a time on the current thread, in
    topological order.
    """
    @override
    def execute(
            self,
            *,
            context: Context,
            workflow: Workflow,
            input: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        context.on_workflow_start(workflow=workflow, input=input)

        node_outputs: Mapping[str, Data] = {}
        ready_nodes = dict(workflow.get_ready_nodes(input=input))
        while len(ready_nodes) > 0:
            node_id, node_input = ready_nodes.popitem()
            node = workflow.nodes_by_id[node_id]

            output = context.on_node_start(node=node, input=node_input)
            if output is None:
                output = node(context, node_input)
                context.on_node_finish(node=node, input=node_input, output=output)
            node_outputs[node.id] = output
            ready_nodes = dict(workflow.get_ready_nodes(
                input=input,
                node_outputs=node_outputs,
                partial_results=ready_nodes,
            ))

        output = workflow.get_output(node_outputs)
        context.on_workflow_finish(
            workflow=workflow,
            input=input,
            output=output,
        )

        return output


__all__ = [
    "ExecutionAlgorithm",
]
