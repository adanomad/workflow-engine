from unittest.mock import AsyncMock

import pytest

from workflow_engine import (
    Edge,
    OutputEdge,
    UserException,
    Workflow,
    WorkflowErrors,
)
from workflow_engine.contexts import InMemoryContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import ConstantStringNode, ErrorNode


def create_error_workflow():
    """Helper function to create the error workflow."""
    return Workflow(
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


def test_workflow_serialization():
    """Test that the error workflow can be serialized and deserialized correctly."""
    workflow = create_error_workflow()
    workflow_json = workflow.model_dump_json()
    deserialized_workflow = Workflow.model_validate_json(workflow_json)
    assert deserialized_workflow == workflow


@pytest.mark.asyncio
async def test_workflow_error_handling():
    """Test that the workflow properly handles errors and calls context callbacks."""
    workflow = create_error_workflow()
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
        node_errors={error_node.id: ["RuntimeError: workflow-engine"]},
    )

    # Verify the output still contains the constant value
    assert output == {"text": "workflow-engine"}

    # Verify on_node_error was called with the correct arguments
    mock_on_node_error.assert_called_once()
    call_args = mock_on_node_error.call_args
    assert call_args.kwargs["node"] is error_node
    exception = call_args.kwargs["exception"]
    assert isinstance(exception, UserException)
    assert exception.message == "RuntimeError: workflow-engine"
