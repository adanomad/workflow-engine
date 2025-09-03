import pytest

from workflow_engine import File, InputEdge, OutputEdge, StringValue, Workflow
from workflow_engine.contexts import InMemoryContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.files import TextFileValue
from workflow_engine.nodes import AppendToFileNode


@pytest.fixture
def workflow():
    """Helper function to create the append workflow."""
    return Workflow(
        nodes=[
            append := AppendToFileNode.from_suffix(
                id="append",
                suffix="_append",
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


@pytest.mark.unit
def test_workflow_serialization(workflow: Workflow):
    """Test that the append workflow can be serialized and deserialized correctly."""
    workflow_json = workflow.model_dump_json(indent=2)
    with open("examples/append.json", "r") as f:
        assert workflow_json == f.read().strip()

    workflow_json = workflow.model_dump_json()
    deserialized_workflow = Workflow.model_validate_json(workflow_json)
    assert deserialized_workflow == workflow


@pytest.mark.asyncio
async def test_workflow_execution(workflow: Workflow):
    """Test that the workflow executes correctly and produces the expected result."""
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    # Create input with a text file
    hello_world = "Hello, world!"
    input_file = TextFileValue(File(path="test.txt"))
    input_file = await input_file.write_text(context, text=hello_world)

    appended_text = StringValue("This text will be appended to the file.")
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
    output_file = output["file"]
    assert isinstance(output_file, TextFileValue)
    assert output_file.path == "test_append.txt"
    output_text = await output_file.read_text(context)
    assert output_text == hello_world + appended_text.root
