# examples/addition.py

# ==============================================================================
# WORKFLOW

from src.workflow_engine.nodes import AddNode, ConstantIntNode
from src.workflow_engine.core import Edge, InputEdge, OutputEdge, Workflow

workflow = Workflow(
    nodes=[
        a := ConstantIntNode.from_value(node_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", value=42),
        b := ConstantIntNode.from_value(node_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", value=2025),
        a_plus_b := AddNode(id="abababab-abab-abab-abab-abababababab"),
        a_plus_b_plus_c := AddNode(id="abcabcab-cabc-abca-bcab-cabcabcabcab"),
    ],
    edges=[
        Edge(source_id=a.id, source_key="value", target_id=a_plus_b.id, target_key="a"),
        Edge(source_id=b.id, source_key="value", target_id=a_plus_b.id, target_key="b"),
        Edge(source_id=a_plus_b.id, source_key="sum", target_id=a_plus_b_plus_c.id, target_key="a"),
    ],
    input_edges=[
        InputEdge(input_key="c", target_id=a_plus_b_plus_c.id, target_key="b"),
    ],
    output_edges=[
        OutputEdge(source_id=a_plus_b_plus_c.id, source_key="sum", output_key="sum"),
    ],
)
workflow_json = workflow.model_dump_json(indent=4)
with open("examples/addition.json", "w") as f:
    f.write(workflow_json)

# make sure serialization roundtrip works
assert Workflow.model_validate_json(workflow_json) == workflow


# ==============================================================================
# CONTEXT

run_id = "11111111-1111-1111-1111-111111111111"

# from .context.in_memory import InMemoryContext
# context = InMemoryContext(
#     run_id=run_id,
# )

# from .context.local import LocalContext
# context = LocalContext(
#     run_id=run_id,
# )

from src.workflow_engine.contexts.supabase import SupabaseContext
context = SupabaseContext(
    run_id=run_id,
    user_id="9dd979c4-6426-40ca-bcaf-7a7f03d550d4",
    workflow_version_id="0eb2c14e-d2a3-4018-b52a-d458328ac2d8",
)


# ==============================================================================
# ALGORITHMS

from src.workflow_engine.execution.topological import TopologicalExecutionAlgorithm
algorithm = TopologicalExecutionAlgorithm()


# ==============================================================================
# EXECUTION

output =algorithm.execute(
    context=context,
    workflow=workflow,
    input={"c": -256},
)
assert output == {"sum": 42 + 2025 - 256}
