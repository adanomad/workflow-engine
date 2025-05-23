import pytest

from workflow_engine import Edge, InputEdge, OutputEdge, Workflow
from workflow_engine.contexts import LocalContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import AddNode, ConstantIntNode


def create_addition_workflow():
    """Helper function to create the addition workflow."""
    return Workflow(
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
            Edge(
                source_id=a.id,
                source_key="value",
                target_id=a_plus_b.id,
                target_key="a",
            ),
            Edge(
                source_id=b.id,
                source_key="value",
                target_id=a_plus_b.id,
                target_key="b",
            ),
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
            OutputEdge(
                source_id=a_plus_b_plus_c.id, source_key="sum", output_key="sum"
            ),
        ],
    )


def test_workflow_serialization():
    """Test that the workflow can be serialized and deserialized correctly."""
    workflow = create_addition_workflow()
    workflow_json = workflow.model_dump_json()
    deserialized_workflow = Workflow.model_validate_json(workflow_json)
    assert deserialized_workflow == workflow


@pytest.mark.asyncio
async def test_workflow_execution():
    """Test that the workflow executes correctly and produces the expected result."""
    workflow = create_addition_workflow()
    context = LocalContext()
    algorithm = TopologicalExecutionAlgorithm()

    c = -256

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={"c": c},
    )
    assert not errors.any()
    assert output == {"sum": 42 + 2025 + c}
