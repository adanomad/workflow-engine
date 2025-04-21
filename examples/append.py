# examples/append.py
from src.workflow_engine.core import TextFile


# ==============================================================================
# WORKFLOW

from src.workflow_engine.nodes import AppendToFileNode, AppendToFileParams
from src.workflow_engine.core import InputEdge, OutputEdge, Workflow

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

run_id = "22222222-2222-2222-2222-222222222222"

from src.workflow_engine.contexts.supabase import SupabaseContext
context = SupabaseContext(
    run_id=run_id,
    user_id="9dd979c4-6426-40ca-bcaf-7a7f03d550d4",
    workflow_version_id="a842f092-0a85-446f-863e-c92ef9c99e67",
    override_paths={"output.txt": "9dd979c4-6426-40ca-bcaf-7a7f03d550d4/bbb5b150-4b25-4cbe-a558-d1b57c76b565"},
)


# ==============================================================================
# ALGORITHMS

from src.workflow_engine.execution.topological import TopologicalExecutionAlgorithm
algorithm = TopologicalExecutionAlgorithm()


# ==============================================================================
# EXECUTION

input = {
    "file": { "path": "output.txt" },
    "text": "This text will be appended to the file.",
}
input_file = TextFile.model_validate(input["file"])
input_text = input_file.read_text(context)

output = algorithm.execute(
    context=context,
    workflow=workflow,
    input=input,
)
assert output == {"file": {"path": "output.txt_append"}}
output_file = TextFile.model_validate(output["file"])
output_text = output_file.read_text(context)

assert output_text == input_text + "This text will be appended to the file."
