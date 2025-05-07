# examples/append.py
from workflow_engine.contexts.supabase import SupabaseContext
from workflow_engine import (
    Edge,
    OutputEdge,
    Workflow,
    WorkflowErrors,
)
from workflow_engine.execution.topological import TopologicalExecutionAlgorithm
from workflow_engine.nodes import (
    ConstantStringNode,
    ErrorNode,
)

# ==============================================================================
# WORKFLOW

workflow = Workflow(
    nodes=[
        constant := ConstantStringNode.from_value(
            node_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            value="workflow-engine",
        ),
        error := ErrorNode.from_name(
            node_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            name="RuntimeError",
        ),
    ],
    edges=[
        Edge.from_nodes(
            source=constant,
            source_key="value",
            target=error,
            target_key="info",
        ),
    ],
    input_edges=[],
    output_edges=[
        OutputEdge.from_node(
            source=constant,
            source_key="value",
            output_key="text",
        ),
    ],
)
workflow_json = workflow.model_dump_json(indent=4)
with open("examples/error.json", "w") as f:
    f.write(workflow_json)

# make sure serialization roundtrip works
assert Workflow.model_validate_json(workflow_json) == workflow


# ==============================================================================
# CONTEXT

run_id = "33333333-3333-3333-3333-000000000006"

context = SupabaseContext(
    run_id=run_id,
    user_id="9dd979c4-6426-40ca-bcaf-7a7f03d550d4",
    workflow_version_id="a842f092-0a85-446f-863e-c92ef9c99e67",
)


# ==============================================================================
# ALGORITHMS


algorithm = TopologicalExecutionAlgorithm()


# ==============================================================================
# EXECUTION

errors, output = algorithm.execute(
    context=context,
    workflow=workflow,
    input={},
)
print("Errors:", errors)
print("Output:", output)
assert errors == WorkflowErrors(
    workflow_errors=[],
    node_errors={error.id: ["RuntimeError: workflow-engine"]},
)
assert output == {"text": "workflow-engine"}
