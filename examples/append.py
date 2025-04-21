# examples/append.py
from uuid import uuid4


# ==============================================================================
# WORKFLOW

from src.workflow_engine.nodes import AppendToFileNode, AppendToFileParams
from src.workflow_engine.core import InputEdge, OutputEdge, Workflow

workflow = Workflow(
    nodes=[
        append := AppendToFileNode(id=str(uuid4()), params=AppendToFileParams(suffix="_append")),
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


# ==============================================================================
# CONTEXT

run_id = str(uuid4())

from src.workflow_engine.contexts.supabase import SupabaseContext
context = SupabaseContext(
    run_id=run_id,
    user_id="9dd979c4-6426-40ca-bcaf-7a7f03d550d4",
    workflow_version_id="0eb2c14e-d2a3-4018-b52a-d458328ac2d8",
    override_paths={"output.txt": "9dd979c4-6426-40ca-bcaf-7a7f03d550d4/bbb5b150-4b25-4cbe-a558-d1b57c76b565"},
)


# ==============================================================================
# ALGORITHMS

from src.workflow_engine.execution.topological import TopologicalExecutionAlgorithm
algorithm = TopologicalExecutionAlgorithm()


# ==============================================================================
# EXECUTION

algorithm.execute(
    context=context,
    workflow=workflow,
    input={
        "file": { "path": "output.txt" },
        "text": "This text will be appended to the file.",
    },
)
