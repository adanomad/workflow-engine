from unittest.mock import AsyncMock

import pytest

from workflow_engine import (
    Edge,
    OutputEdge,
    StringValue,
    UserException,
    Workflow,
    WorkflowErrors,
)
from workflow_engine.contexts import InMemoryContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import ConstantStringNode, ErrorNode


@pytest.fixture
def workflow():
    """Helper function to create the error workflow."""
    return Workflow(
        nodes=[
            constant := ConstantStringNode.from_value(id="constant", value="test"),
            error := ErrorNode.from_name(id="error", name="RuntimeError"),
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


@pytest.mark.unit
def test_workflow_serialization(workflow: Workflow):
    """Test that the error workflow can be serialized and deserialized correctly."""
    workflow_json = workflow.model_dump_json(indent=2)
    with open("examples/error.json", "r") as f:
        assert workflow_json == f.read().strip()

    deserialized_workflow = Workflow.model_validate_json(workflow_json)
    assert deserialized_workflow == workflow


@pytest.mark.asyncio
async def test_workflow_error_handling(workflow: Workflow):
    """Test that the workflow properly handles errors and calls context callbacks."""
    context = InMemoryContext()

    # Create a mock for on_node_error while preserving the original function
    original_on_node_error = context.on_node_error
    mock_on_node_error = AsyncMock(side_effect=original_on_node_error)
    context.on_node_error = mock_on_node_error

    algorithm = TopologicalExecutionAlgorithm()

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={},
    )

    error_node = workflow.nodes[1]
    # Verify the error was captured correctly
    assert errors == WorkflowErrors(
        workflow_errors=[],
        node_errors={error_node.id: ["RuntimeError: test"]},
    )

    # Verify the output still contains the constant value
    assert output == {"text": StringValue("test")}

    # Verify on_node_error was called with the correct arguments
    mock_on_node_error.assert_called_once()
    call_args = mock_on_node_error.call_args
    assert call_args.kwargs["node"] is error_node
    exception = call_args.kwargs["exception"]
    assert isinstance(exception, UserException)
    assert exception.message == "RuntimeError: test"
