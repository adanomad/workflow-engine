import pytest

from workflow_engine import InputEdge, OutputEdge, Workflow
from workflow_engine.contexts import InMemoryContext
from workflow_engine.files import TextFile
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import (
    AppendToFileNode,
    AppendToFileParams,
)


def create_append_workflow():
    """Helper function to create the append workflow."""
    return Workflow(
        nodes=[
            append := AppendToFileNode(
                id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                params=AppendToFileParams(suffix="_append"),
            ),
        ],
        edges=[],
        input_edges=[
            InputEdge(input_key="text", target_id=append.id, target_key="text"),
            InputEdge(input_key="file", target_id=append.id, target_key="file"),
        ],
        output_edges=[
            OutputEdge(source_id=append.id, source_key="file", output_key="file"),
        ],
    )


def test_workflow_serialization():
    """Test that the append workflow can be serialized and deserialized correctly."""
    workflow = create_append_workflow()
    workflow_json = workflow.model_dump_json()
    deserialized_workflow = Workflow.model_validate_json(workflow_json)
    assert deserialized_workflow == workflow


@pytest.mark.asyncio
async def test_workflow_execution():
    """Test that the workflow executes correctly and produces the expected result."""
    workflow = create_append_workflow()
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    # Create input with a text file
    hello_world = "Hello, world!"
    input_file = TextFile(path="test.txt")
    input_file = await input_file.write_text(context, text=hello_world)

    appended_text = "This text will be appended to the file."
    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "file": input_file,
            "text": appended_text,
        },
    )

    # Verify no errors occurred
    assert not errors.any()

    # Verify the output file exists and has the correct content
    output_file = TextFile.model_validate(output["file"])
    assert output_file.path == "test_append.txt"
    output_text = await output_file.read_text(context)
    assert output_text == hello_world + appended_text
