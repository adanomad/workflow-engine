# examples/addition.py
from workflow_engine.contexts.local import LocalContext
from workflow_engine.core import Edge, InputEdge, OutputEdge, Workflow
from workflow_engine.execution.topological import TopologicalExecutionAlgorithm
from workflow_engine.nodes import AddNode, ConstantIntNode

# ==============================================================================
# WORKFLOW


workflow = Workflow(
    nodes=[
        a := ConstantIntNode.from_value(
            node_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", value=42
        ),
        b := ConstantIntNode.from_value(
            node_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", value=2025
        ),
        a_plus_b := AddNode(id="abababab-abab-abab-abab-abababababab"),
        a_plus_b_plus_c := AddNode(id="abcabcab-cabc-abca-bcab-cabcabcabcab"),
    ],
    edges=[
        Edge(source_id=a.id, source_key="value", target_id=a_plus_b.id, target_key="a"),
        Edge(source_id=b.id, source_key="value", target_id=a_plus_b.id, target_key="b"),
        Edge(
            source_id=a_plus_b.id,
            source_key="sum",
            target_id=a_plus_b_plus_c.id,
            target_key="a",
        ),
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

context = LocalContext()


# ==============================================================================
# ALGORITHMS

algorithm = TopologicalExecutionAlgorithm()


# ==============================================================================
# EXECUTION

errors, output = algorithm.execute(
    context=context,
    workflow=workflow,
    input={"c": -256},
)
assert not errors
assert output == {"sum": 42 + 2025 - 256}
