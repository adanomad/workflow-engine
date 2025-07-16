import pytest

from workflow_engine import InputEdge, OutputEdge, Workflow
from workflow_engine.contexts import InMemoryContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import AddNode, ForEachNode


@pytest.fixture
def add_workflow() -> Workflow:
    """Create a workflow that adds two numbers: a + b = c."""
    return Workflow(
        nodes=[
            add := AddNode(id="add"),
        ],
        edges=[],
        input_edges=[
            InputEdge.from_node(
                input_key="a",
                target=add,
                target_key="a",
            ),
            InputEdge.from_node(
                input_key="b",
                target=add,
                target_key="b",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=add,
                source_key="sum",
                output_key="c",
            ),
        ],
    )


@pytest.fixture
def workflow(add_workflow: Workflow) -> Workflow:
    workflow = Workflow(
        nodes=[
            for_each := ForEachNode.from_workflow(
                id="for_each",
                workflow=add_workflow,
            ),
        ],
        edges=[],
        input_edges=[
            InputEdge.from_node(
                input_key="sequence",
                target=for_each,
                target_key="sequence",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=for_each,
                source_key="sequence",
                output_key="results",
            ),
        ],
    )

    return workflow


@pytest.mark.asyncio
async def test_for_each_simple_sequence(workflow: Workflow):
    """Test that ForEachNode processes a simple sequence of addition operations."""
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    input = workflow.input_type.model_validate(
        {
            "sequence": [
                {"a": 1.0, "b": 2.0},
                {"a": 3.0, "b": 4.0},
                {"a": 5.0, "b": 6.0},
            ]
        }
    ).to_dict()

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input=input,
    )

    assert not errors.any(), errors

    assert (
        output
        == workflow.output_type.model_validate(
            {
                "results": [
                    {"c": 3.0},
                    {"c": 7.0},
                    {"c": 11.0},
                ]
            }
        ).to_dict()
    )


@pytest.mark.asyncio
async def test_for_each_empty(workflow: Workflow):
    """Test that ForEachNode processes an empty sequence."""
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    input = workflow.input_type.model_validate(
        {
            "sequence": [],
        }
    ).to_dict()

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input=input,
    )

    assert not errors.any(), errors

    assert (
        output
        == workflow.output_type.model_validate(
            {
                "results": [],
            }
        ).to_dict()
    )
