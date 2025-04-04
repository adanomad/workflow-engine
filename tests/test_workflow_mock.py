# tests/test_workflow_mock.py

import pytest
from unittest.mock import AsyncMock, MagicMock, call, ANY
import uuid
from workflow_engine.workflow import WorkflowExecutor
from workflow_engine.resolvers import BaseResolver
from workflow_engine.types import (
    File,
    FileExecutionData,
    WorkflowGraph,
    Node,
    Edge,
)
from typing import List
from workflow_engine.registry import registry
from workflow_engine.functions import builtins
from dotenv import load_dotenv

load_dotenv()

# --- Unit Tests ---
pytestmark = pytest.mark.unit


@pytest.fixture
def mock_supabase_client():
    """Provides a mock Supabase client."""
    mock_client = MagicMock()
    mock_client.table = MagicMock()
    mock_client.storage = MagicMock()
    mock_client.storage.from_ = MagicMock()
    return mock_client


@pytest.fixture
async def mock_resolver():
    """Provides a SupabaseResolver with mocked Supabase client and methods."""
    # Patch the create_async_client call within the resolver module
    resolver = AsyncMock(spec=BaseResolver)
    resolver.initialize = AsyncMock()

    # Default behavior (can be overridden in tests)
    resolver.get_function_config.return_value = {}  # Default to no config

    # Mock get_function to return actual registered functions
    def _get_func_side_effect(reference_id: str):
        # In tests, we'll often use the function name as the reference_id
        func = registry.get_function(reference_id)
        if func:
            return func
        else:
            raise ValueError(
                f"Mock Resolver: Function '{reference_id}' not found in registry."
            )

    resolver.get_function.side_effect = _get_func_side_effect

    return resolver


@pytest.fixture
def workflow_executor(mock_resolver):
    """Provides a WorkflowExecutor instance with the mocked resolver."""
    return WorkflowExecutor(resolver=mock_resolver)


# --- Helper Function ---
def create_mock_file_execution_data(
    node_id: str,  # Node that "produced" this
    mime_type: str = "text/plain",
    content: str = "Default content",
    title_prefix: str = "source_file",
) -> FileExecutionData:
    """Creates a FileExecutionData object for mocking get_node_files return."""
    file_id = str(uuid.uuid4())
    user_id = "mock_user_id"

    metadata = File(
        id=file_id,
        user=user_id,
        title=f"{title_prefix}_{node_id}.{mime_type.split('/')[-1]}",
        file_type=mime_type,
        file_size=len(content.encode("utf-8")),
        metadata={"source_node": node_id},
    )
    return FileExecutionData(metadata=metadata, content=content.encode("utf-8"))


def create_final_file_metadata(
    exec_data: FileExecutionData, saved_by_node: str
) -> File:
    """Creates a File object simulating the result after saving."""
    # Simulate DB assigning final ID, timestamp, user etc.
    # Often, the ID might be the same if generated beforehand, but let's assume DB generates
    final_id = str(uuid.uuid4())
    final_user = "resolver_user_id"  # Simulate resolver setting user

    # Create a new File object based on input metadata but with updated fields
    final_meta = exec_data.metadata.model_copy(
        update={
            "id": final_id,
            "user": final_user,
            "file_size": (
                len(exec_data.content) if isinstance(exec_data.content, bytes) else None
            ),
            "metadata": {
                **(exec_data.metadata.metadata or {}),
                "saved_by_node": saved_by_node,
            },
        }
    )
    # Recalculate size if content is file-like (simplification: assume bytes for mock)
    if not final_meta.file_size and isinstance(exec_data.content, bytes):
        final_meta.file_size = len(exec_data.content)

    return final_meta


# --- Test Cases ---


@pytest.mark.asyncio
async def test_linear_workflow_text_to_json(workflow_executor, mock_resolver):
    """Tests a simple generate -> process workflow."""
    # 1. Define Workflow Graph
    node1_id = "node_gen_text"
    node2_id = "node_proc_json"
    workflow_def = {
        "nodes": [
            Node(
                id=node1_id,
                name="Generate Text",
                reference_id="generate_text_file",
            ).model_dump(),
            Node(
                id=node2_id,
                name="Process JSON",
                reference_id="process_text_to_json",
            ).model_dump(),
        ],
        "edges": [
            Edge(
                id="edge1",
                source=node1_id,
                target=node2_id,
                sourceHandle="text/plain",
                targetHandle="text/plain",
                target_parameter="input_files",
            ).model_dump(by_alias=True)
        ],
    }

    # 2. Configure Mocks
    #   - get_function_config: Not needed for these nodes, default {} is fine.
    #   - get_node_files: Not called for node1 (source). Called for node2.
    #   - save_node_results: Called for node1 and node2 outputs.

    # Mock save_node_results to simulate saving and return final File metadata
    async def mock_save_side_effect(
        node_id: str, results: List[FileExecutionData], run_id: str
    ):
        print(
            f"Mock Save called for Node: {node_id} in Run: {run_id} with {len(results)} items"
        )
        final_files = [
            create_final_file_metadata(exec_data, node_id) for exec_data in results
        ]
        return final_files

    mock_resolver.save_node_results.side_effect = mock_save_side_effect

    # add mock for get_node_files on node2
    mock_node1_output_exec_data = [
        create_mock_file_execution_data(
            node_id=node1_id,
            mime_type="text/plain",
            content="This is line one.\nThis is line two.",
            title_prefix="output",
        )
    ]

    async def mock_get_files_side_effect(node_id: str, mime_type: str, run_id: str):
        print(
            f"Mock get_node_files called with: node_id={node_id}, mime_type={mime_type}, run_id={run_id}"
        )
        if node_id == node1_id and mime_type == "text/plain":
            print(f"Mock get_node_files returning simulated output for {node1_id}")
            return mock_node1_output_exec_data
        print(f"Mock get_node_files returning default [] for {node_id}")
        return []

    mock_resolver.get_node_files.side_effect = mock_get_files_side_effect

    # 3. Load and Execute
    workflow_executor.load_workflow(workflow_def)
    run_id, final_results = (
        await workflow_executor.execute()
    )  # Returns Dict[str, List[File]]

    # 4. Assertions
    #   - Check final_results structure and content (should contain final File metadata)
    assert node1_id in final_results
    assert node2_id in final_results
    assert isinstance(final_results[node1_id], list)
    assert (
        len(final_results[node1_id]) == 1
    )  # generate_text_file produces 1 file by default
    assert isinstance(final_results[node1_id][0], File)
    assert final_results[node1_id][0].file_type == "text/plain"
    assert final_results[node1_id][0].title == "output.txt"
    assert final_results[node1_id][0].metadata.get("saved_by_node") == node1_id
    assert isinstance(run_id, str) and len(run_id) > 0

    assert isinstance(final_results[node2_id], list)
    assert (
        len(final_results[node2_id]) == 1
    )  # process_text_to_json processes 1 input -> 1 output
    assert isinstance(final_results[node2_id][0], File)
    assert final_results[node2_id][0].file_type == "application/json"
    assert final_results[node2_id][0].title == "processed_0.json"
    assert final_results[node2_id][0].metadata.get("saved_by_node") == node2_id
    assert "original_file_id" in final_results[node2_id][0].metadata

    mock_resolver.get_function.assert_has_calls(
        [call("generate_text_file"), call("process_text_to_json")], any_order=True
    )
    mock_resolver.get_node_files.assert_called_once_with(
        node_id=node1_id, mime_type="text/plain", run_id=ANY
    )
    assert mock_resolver.save_node_results.call_count == 2
    #     - Check args passed to save for node 1 (example check)
    save_call_node1 = mock_resolver.save_node_results.call_args_list[0]
    assert save_call_node1.args[0] == node1_id  # First arg is node_id
    assert isinstance(save_call_node1.args[1], list)
    assert len(save_call_node1.args[1]) == 1
    assert isinstance(save_call_node1.args[1][0], FileExecutionData)
    assert save_call_node1.args[1][0].metadata.file_type == "text/plain"
    assert isinstance(save_call_node1.args[2], str) and len(save_call_node1.args[2]) > 0


@pytest.mark.asyncio
async def test_workflow_with_config(workflow_executor, mock_resolver):
    """Tests a workflow where a node uses configuration parameters."""
    node1_id = "node_gen_multi_line"
    node2_id = "node_analyze"

    nodes = [
        Node(
            id=node1_id,
            name="Generate Multi-Line",
            reference_id="generate_text_file",
        ).model_dump(),
        Node(
            id=node2_id,
            name="Analyze Data",
            reference_id="analyze_json_data",
        ).model_dump(),
        Node(
            id="node_proc",
            name="Text to JSON",
            reference_id="process_text_to_json",
        ).model_dump(),
    ]
    edges = [
        Edge(
            id="e1",
            source=node1_id,
            target="node_proc",
            sourceHandle="text/plain",
            targetHandle="text/plain",
            target_parameter="input_files",
        ).model_dump(by_alias=True),
        Edge(
            id="e2",
            source="node_proc",
            target=node2_id,
            sourceHandle="application/json",
            targetHandle="application/json",
            target_parameter="json_inputs",
        ).model_dump(by_alias=True),
    ]
    workflow_def: WorkflowGraph = {"nodes": nodes, "edges": edges}

    # Configure Mocks
    #   - Mock get_function_config for the nodes that need it
    node1_config = {"base_name": "multi_line_doc", "line_count": 5}
    node2_config = {"report_title": "My Custom Analysis", "min_lines_threshold": 3}

    async def mock_get_config(node_data: Node):
        node_id = node_data.id
        if node_id == node1_id:
            return node1_config
        if node_id == node2_id:
            return node2_config
        return {}  # Default empty config

    mock_resolver.get_function_config.side_effect = mock_get_config

    #   - Mock save_node_results (same as previous test)
    async def mock_save_side_effect(
        node_id: str, results: List[FileExecutionData], run_id: str
    ):
        print(
            f"Mock Save called for Node: {node_id} in Run: {run_id} with {len(results)} items"
        )
        final_files = [
            create_final_file_metadata(exec_data, node_id) for exec_data in results
        ]
        return final_files

    mock_resolver.save_node_results.side_effect = mock_save_side_effect

    # add mock for get_node_files on node2
    mock_node1_output_exec_data = [
        create_mock_file_execution_data(
            node_id=node1_id,
            mime_type="text/plain",
            content="This is line one.\nThis is line two.",
            title_prefix="output",
        )
    ]

    async def mock_get_files_side_effect(node_id: str, mime_type: str, run_id: str):
        print(
            f"Mock get_node_files called with: node_id={node_id}, mime_type={mime_type}, run_id={run_id}"
        )
        if node_id == node1_id and mime_type == "text/plain":
            print(f"Mock get_node_files returning simulated output for {node1_id}")
            return mock_node1_output_exec_data
        print(f"Mock get_node_files returning default [] for {node_id}")
        return []

    mock_resolver.get_node_files.side_effect = mock_get_files_side_effect

    # Load and Execute
    workflow_executor.load_workflow(workflow_def)
    run_id, final_results = await workflow_executor.execute()

    # Assertions
    assert node1_id in final_results
    assert "node_proc" in final_results
    assert node2_id in final_results
    assert isinstance(run_id, str) and len(run_id) > 0

    # Check output of node 1 reflects config
    assert final_results[node1_id][0].title == "multi_line_doc.txt"
    assert final_results[node1_id][0].metadata.get("lines") == 5

    # Check output of node 2 reflects config
    assert final_results[node2_id][0].file_type == "text/plain"
    assert final_results[node2_id][0].title == "My_Custom_Analysis.txt"
    assert final_results[node2_id][0].metadata.get("threshold") == 3

    # Verify get_function_config was called for each node
    assert mock_resolver.get_function_config.call_count == 3
    mock_resolver.get_function_config.assert_has_calls(
        [
            call(Node(**nodes[0])),
            call(Node(**nodes[1])),
            call(Node(**nodes[2])),
        ],
        any_order=True,
    )

    # Check get_node_files calls
    mock_resolver.get_node_files.assert_has_calls(
        [
            call(node_id=node1_id, mime_type="text/plain", run_id=ANY),
            call(node_id="node_proc", mime_type="application/json", run_id=ANY),
        ],
        any_order=True,
    )

    # Check save calls
    assert mock_resolver.save_node_results.call_count == 3


# TODO: Add tests for:
# - Resolver errors during get_node_files or save_node_results
# - Missing required parameter from config
# - Node function raising an exception during execution
# - Invalid workflow graph (e.g., cycle detection tested in load_workflow)
# - More complex graphs (multiple inputs/outputs)
