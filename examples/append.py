# examples/append.py
from workflow_engine.contexts.supabase import SupabaseContext
from workflow_engine import (
    InputEdge,
    OutputEdge,
    Workflow,
    WorkflowExecutionError,
)
from workflow_engine.core import TextFile
from workflow_engine.execution.topological import TopologicalExecutionAlgorithm
from workflow_engine.nodes import AppendToFileNode, AppendToFileParams

# ==============================================================================
# WORKFLOW

workflow = Workflow(
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
workflow_json = workflow.model_dump_json(indent=4)
with open("examples/append.json", "w") as f:
    f.write(workflow_json)

# make sure serialization roundtrip works
assert Workflow.model_validate_json(workflow_json) == workflow


# ==============================================================================
# CONTEXT

run_id = "22222222-2222-2222-2222-222222222227"

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

input = {
    # an existing file in the database with the contents "Hello, world!"
    "file": {
        "path": "output.txt",
        "metadata": {
            "file_id": "bbb5b150-4b25-4cbe-a558-d1b57c76b565",
        },
    },
    "text": "This text will be appended to the file.",
}
input_file = TextFile.model_validate(input["file"])
input_text = input_file.read_text(context)

output = algorithm.execute(
    context=context,
    workflow=workflow,
    input=input,
)
assert not isinstance(output, WorkflowExecutionError)
print(output)

output_file = TextFile.model_validate(output["file"])
assert output_file.path == "output_append.txt"
output_text = output_file.read_text(context)
print(output_text)

assert output_text == input_text + "This text will be appended to the file."
