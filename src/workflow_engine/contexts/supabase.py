# workflow_engine/contexts/supabase.py
from typing import Any, Mapping, TypeVar

from supabase import create_client

from ..core import Context, Data, File, Node, Workflow
from ..utils.env import get_env
from ..utils.iter import only
from ..utils.uuid import is_valid_uuid


F = TypeVar("F", bound=File)


class SupabaseContext(Context):
    """
    A context that stores node/workflow inputs/outputs in Supabase tables, and
    writes files to a Supabase bucket.

    The file_metadata_table should have:
    - user: the user id
    - title: the title of the file
    - file_type: the type of the file
    - file_size: the size of the file

    The workflow_runs table should have:
    - id: the id of the workflow run
    - workflow_id: the id of the workflow
    - input: the JSON input of the workflow
    - output: the JSON output of the workflow
    - started_at: the timestamp when the workflow started
    - finished_at: the timestamp when the workflow finished

    The workflow_node_runs table should have:
    - workflow_run_id: the id of the workflow run
    - node_id: the id of the node
    - input: the JSON input of the node
    - output: the JSON output of the node
    - started_at: the timestamp when the node started
    - finished_at: the timestamp when the node finished

    Parameters:
    - override_paths: a mapping from file paths to file IDs
    """
    def __init__(
            self,
            run_id: str,
            *,
            user_id: str,
            workflow_version_id: str,
            file_metadata_table: str = "document_info",
            file_bucket: str = "documents_storage",
            workflow_runs_table: str = "workflow_runs",
            workflow_node_runs_table: str = "workflow_node_runs",
            overwrites_allowed: bool = False,
            supabase_url: str | None = None,
            supabase_key: str | None = None,
    ):
        super().__init__(run_id=run_id)

        if supabase_url is None:
            supabase_url = get_env("SUPABASE_URL")
        if supabase_key is None:
            supabase_key = get_env("SUPABASE_SERVICE_KEY")

        self.supabase = create_client(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )
        self.user_id = user_id
        self.workflow_version_id = workflow_version_id
        self.file_metadata_table_name = file_metadata_table
        self.file_bucket_name = file_bucket
        self.workflow_runs_table_name = workflow_runs_table
        self.workflow_node_runs_table_name = workflow_node_runs_table
        self.overwrites_allowed = overwrites_allowed

    @property
    def file_metadata_table(self):
        return self.supabase.table(self.file_metadata_table_name)

    @property
    def file_bucket(self):
        return self.supabase.storage.from_(self.file_bucket_name)

    @property
    def workflow_runs_table(self):
        return self.supabase.table(self.workflow_runs_table_name)

    @property
    def workflow_node_runs_table(self):
        return self.supabase.table(self.workflow_node_runs_table_name)

    def get_env(self, key: str, default: str | None = None) -> str:
        """
        Maybe in the long run we will have a
        """
        return get_env(key, default=default)

    def get_file_id(self, file: File) -> str | None:
        if "file_id" in file.metadata:
            file_id = file.metadata["file_id"]
            assert isinstance(file_id, str)
            return file_id
        elif is_valid_uuid(file.path):
            return file.path
        else:
            return None

    def read(
            self,
            file: File,
    ) -> bytes:
        file_id = self.get_file_id(file)
        if file_id is None:
            raise ValueError(f"File {file.path} not found")
        content = self.file_bucket.download(f"{self.user_id}/{file_id}")
        return content

    def write(
            self,
            file: F,
            content: bytes,
    ) -> F:
        insert_dict = {
            "user": self.user_id,
            "title": f"{self.run_id}/{file.path}",
            "file_type": file.mime_type,
            "file_size": len(content),
        }
        if (file_id := self.get_file_id(file)) is not None:
            insert_dict["id"] = file_id
        response = self.file_metadata_table.insert(insert_dict).execute()
        file_id = only(response.data)["id"]
        self.file_bucket.upload(
            path=f"{self.user_id}/{file_id}",
            file=content,
            file_options={
                "content_type": file.mime_type, # type: ignore
                "upsert": "true" if self.overwrites_allowed else "false",
            },
        )
        return file.write_metadata("file_id", file_id)

    def on_workflow_start(
            self,
            *,
            workflow: Workflow,
            input: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """
        A hook that is called when a workflow starts execution.

        If the context already knows what the workflow's output will be, return
        that output to skip workflow execution.
        """
        response = (
            self.workflow_runs_table
                .select("*")
                .eq("id", self.run_id)
                .execute()
        )
        if len(response.data) > 0:
            output = only(response.data)["output"]
            if output is not None:
                return output

        self.workflow_runs_table.upsert({
            "id": self.run_id,
            "workflow_id": self.workflow_version_id,
            "input": input,
            "started_at": "now()",
            "output": None,
            "finished_at": None,
        }).execute()
        return None

    def on_node_start(
            self,
            *,
            node: Node,
            input: Data,
    ) -> Data | None:
        """
        A hook that is called when a node starts execution.

        If the context already knows what the node's output will be, return that
        output to skip node execution.
        """
        response = (
            self.workflow_node_runs_table
                .select("*")
                .eq("workflow_run_id", self.run_id)
                .eq("node_id", node.id)
                .execute()
        )
        if len(response.data) > 0:
            output = only(response.data)["output"]
            if output is not None:
                return node.output_type.model_validate(output)

        self.workflow_node_runs_table.upsert({
            "workflow_run_id": self.run_id,
            "node_id": node.id,
            "input": input.model_dump(),
            "started_at": "now()",
            "output": None,
            "finished_at": None,
        }).execute()
        return None

    def on_node_finish(
            self,
            *,
            node: "Node",
            input: Data,
            output: Data,
    ) -> Data:
        """
        A hook that is called when a node finishes execution.
        """
        (
            self.workflow_node_runs_table
                .update({
                    "output": output.model_dump(),
                    "finished_at": "now()",
                })
                .eq("workflow_run_id", self.run_id)
                .eq("node_id", node.id)
                .execute()
        )
        return output

    def on_workflow_finish(
            self,
            *,
            workflow: "Workflow",
            input: Mapping[str, Any],
            output: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        A hook that is called when a workflow finishes execution.
        """
        (
            self.workflow_runs_table
                .update({
                    "output": output,
                    "finished_at": "now()",
                })
                .eq("id", self.run_id)
                .execute()
        )
        return output


__all__ = [
    "SupabaseContext",
]
