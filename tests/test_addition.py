import pytest

from workflow_engine import Edge, InputEdge, IntegerValue, OutputEdge, Workflow
from workflow_engine.contexts import InMemoryContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import AddNode, ConstantIntegerNode


@pytest.fixture
def workflow():
    """Helper function to create the addition workflow."""
    return Workflow(
        nodes=[
            a := ConstantIntegerNode.from_value(id="a", value=42),
            b := ConstantIntegerNode.from_value(id="b", value=2025),
            a_plus_b := AddNode(id="a+b"),
            a_plus_b_plus_c := AddNode(id="a+b+c"),
        ],
        edges=[
            Edge.from_nodes(
                source=a,
                source_key="value",
                target=a_plus_b,
                target_key="a",
            ),
            Edge.from_nodes(
                source=b,
                source_key="value",
                target=a_plus_b,
                target_key="b",
            ),
            Edge.from_nodes(
                source=a_plus_b,
                source_key="sum",
                target=a_plus_b_plus_c,
                target_key="a",
            ),
        ],
        input_edges=[
            InputEdge.from_node(
                input_key="c",
                target=a_plus_b_plus_c,
                target_key="b",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=a_plus_b_plus_c,
                source_key="sum",
                output_key="sum",
            ),
        ],
    )


@pytest.mark.unit
def test_workflow_serialization(workflow: Workflow):
    """Test that the workflow can be serialized and deserialized correctly."""
    workflow_json = workflow.model_dump_json(indent=2)
    with open("examples/addition.json", "r") as f:
        assert workflow_json == f.read().strip()

    workflow_json = workflow.model_dump_json()
    deserialized_workflow = Workflow.model_validate_json(workflow_json)
    assert deserialized_workflow == workflow


@pytest.mark.asyncio
async def test_workflow_execution(workflow: Workflow):
    """Test that the workflow executes correctly and produces the expected result."""
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    c = -256

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={"c": IntegerValue(c)},
    )
    assert not errors.any()
    assert output == {"sum": 42 + 2025 + c}
