# tests/test_workflow_supabase_integration.py

import pytest
import uuid
from dotenv import load_dotenv

load_dotenv()

from workflow_engine.workflow import WorkflowExecutor, WorkflowExecutionError
from workflow_engine.resolvers import SupabaseResolver
from workflow_engine.types import Node, Edge, Position
from workflow_engine.functions import builtins
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# fixed workflow_id for testing
WORKFLOW_ID = "1a41f60e-acb8-4687-b899-7c37eb892535"

# --- Integration Test ---


# Mark this test as 'integration' to potentially skip it during unit testing
# Run with: pytest -m integration
# Run with logs: pytest -s -m integration tests/test_workflow_integration.py
@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_real_workflow_on_supabase():
    """
    Tests the full workflow execution using a real Supabase instance.
    Verifies data creation in DB and Storage by manual inspection.
    """
    print("\n--- Starting Supabase Integration Test ---")

    # 1. Define Workflow Graph using real Tool UUIDs
    NODE_GENERATE_ID = "11111111-1111-1111-1111-111111111111"
    NODE_PROCESS_ID = "22222222-2222-2222-2222-222222222222"
    NODE_ANALYZE_ID = "33333333-3333-3333-3333-333333333333"

    workflow_def = {
        "nodes": [
            Node(
                id=NODE_GENERATE_ID,
                name="Generate Text",
                reference_id="generate_text_file",
                position=Position(x=10, y=10),
            ).model_dump(),
            Node(
                id=NODE_ANALYZE_ID,
                name="Analyze JSON",
                reference_id="analyze_json_data",
                position=Position(x=400, y=10),
            ).model_dump(),
            Node(
                id=NODE_PROCESS_ID,
                name="Text to JSON",
                reference_id="process_text_to_json",
                position=Position(x=200, y=10),
            ).model_dump(),
        ],
        "edges": [
            Edge(
                id=f"edge_{uuid.uuid4()}",
                source=NODE_GENERATE_ID,
                target=NODE_PROCESS_ID,
                mime_type="text/plain",
                target_parameter="input_files",
            ).model_dump(by_alias=True),
            Edge(
                id=f"edge_{uuid.uuid4()}",
                source=NODE_PROCESS_ID,
                target=NODE_ANALYZE_ID,
                mime_type="application/json",
                target_parameter="json_inputs",
            ).model_dump(by_alias=True),
        ],
    }

    # 2. Instantiate Real Resolver
    resolver = await SupabaseResolver(
        user_id="1a72f66c-e892-46b5-ab9f-17c3017df5ed",
        storage_bucket="documents_storage",
    ).initialize()
    print("SupabaseResolver instantiated successfully.")

    # # 3. (OPTIONAL) Pre-configure Nodes in Supabase with empty configs
    # try:
    #     node_analyze_config = {
    #         "report_title": "Integration Test Report",
    #         "min_lines_threshold": 2,
    #     }

    #     node_configs = [
    #         {
    #             "node_id_ref": NODE_GENERATE_ID,
    #             "config": {},
    #             "workflow_id": WORKFLOW_ID,
    #         },
    #         {
    #             "node_id_ref": NODE_PROCESS_ID,
    #             "config": {},
    #             "workflow_id": WORKFLOW_ID,
    #         },
    #         {
    #             "node_id_ref": NODE_ANALYZE_ID,
    #             "config": node_analyze_config,
    #             "workflow_id": WORKFLOW_ID,
    #         },
    #     ]

    #     for config in node_configs:
    #         config_response = (
    #             await resolver.supabase.table("node_workspaces")
    #             .upsert(config)
    #             .execute()
    #         )
    #         if config_response.data:
    #             print(f"Configuration set for node {config['node_id_ref']}")
    #         else:
    #             print(
    #                 f"WARN: Configuration might not have been set for node {config['node_id_ref']}. Response: {getattr(config_response, 'error', 'No data')}"
    #             )

    # except Exception as e:
    #     pytest.fail(f"Failed to pre-configure nodes in Supabase: {e}")

    # 4. Load and Execute
    try:
        executor = WorkflowExecutor(resolver=resolver)
        print("Loading workflow...")
        executor.load_workflow(workflow_def)
        print("Executing workflow...")
        run_id, final_results = await executor.execute()
        print("Workflow execution finished.")
        print("--- Final Results (Persisted File Metadata) ---")
        print(final_results)
        assert final_results is not None
        assert isinstance(run_id, str) and len(run_id) > 0
        assert NODE_GENERATE_ID in final_results
        assert NODE_PROCESS_ID in final_results
        assert NODE_ANALYZE_ID in final_results
        assert (
            len(final_results[NODE_ANALYZE_ID]) > 0
        )  # Analyze should produce a report

    except WorkflowExecutionError as e:
        pytest.fail(
            f"Workflow execution failed: {e}\nOriginal Exception: {e.original_exception}"
        )
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during workflow execution: {e}")

    # 5. Manual Verification Steps (Instructions for the user)
    # print("\n--- Verification Steps (Manual) ---")
    # print("1. Table `document_info`:")
    # print(
    #     "   - Look for rows with titles like 'output.txt', 'processed_0.json', 'Integration_Test_Report.txt'."
    # )
    # print(f"   - Check IDs matching those printed in 'Final Results' above.")
    # print("   - Verify `file_type`, `file_size`, `metadata`, `user` columns.")
    # print("2. Table `node_workspace_docs`:")
    # print(
    #     f"   - Look for rows linking node IDs ({node_gen_id}, {node_proc_id}, {node_analyze_id}) to the file IDs created in `document_info`."
    # )
    # print("3. Storage Bucket (`documents_storage` or your configured name):")
    # print(
    #     f"   - Navigate to the paths corresponding to the created files (e.g., '{resolver.user_id}/<file_id_from_document_info>')."
    # )
    # print(
    #     "   - Download the files and verify their content (e.g., check the report text, the JSON structure)."
    # )
    # print("--- Integration Test Complete ---")

    # 6. Cleanup (Optional - Manual for now)
    # print("\n--- Cleanup (Manual) ---")
    # print("Remember to manually delete the created rows in `document_info`, `node_workspace_docs`")
    # print("and the corresponding files in the Storage bucket if desired.")
