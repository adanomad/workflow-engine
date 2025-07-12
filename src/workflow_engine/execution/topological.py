# workflow_engine/execution/topological.py

from collections.abc import Mapping

from overrides import override

from ..core import Context, DataMapping, ExecutionAlgorithm, Workflow, WorkflowErrors


class TopologicalExecutionAlgorithm(ExecutionAlgorithm):
    """
    Executes the workflow one node at a time on the current thread, in
    topological order.
    """

    @override
    async def execute(
        self,
        *,
        context: Context,
        workflow: Workflow,
        input: DataMapping,
    ) -> tuple[WorkflowErrors, DataMapping]:
        result = await context.on_workflow_start(workflow=workflow, input=input)
        if result is not None:
            # TODO: maybe retry workflows that have failed
            return result

        node_outputs: Mapping[str, DataMapping] = {}
        errors = WorkflowErrors()

        try:
            ready_nodes = dict(workflow.get_ready_nodes(input=input))
            while len(ready_nodes) > 0:
                node_id, node_input = ready_nodes.popitem()
                node = workflow.nodes_by_id[node_id]

                node_outputs[node.id] = await node(context, node_input)
                ready_nodes = dict(
                    workflow.get_ready_nodes(
                        input=input,
                        node_outputs=node_outputs,
                        partial_results=ready_nodes,
                    )
                )

            output = workflow.get_output(node_outputs)
        except Exception as e:
            errors.add(e)
            partial_output = workflow.get_output(node_outputs, partial=True)
            errors, partial_output = await context.on_workflow_error(
                workflow=workflow,
                input=input,
                errors=errors,
                partial_output=partial_output,
            )
            return errors, partial_output

        output = await context.on_workflow_finish(
            workflow=workflow,
            input=input,
            output=output,
        )

        return errors, output


__all__ = [
    "TopologicalExecutionAlgorithm",
]
