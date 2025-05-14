# workflow_engine/contexts/local.py
import json
import os
from collections.abc import Mapping
from typing import Any, TypeVar

from pydantic import BaseModel

from workflow_engine.core.error import UserException, WorkflowErrors

from ..core import Context, File, Node, Workflow

F = TypeVar("F", bound=File)


class LocalContext(Context):
    """
    A context that uses the local filesystem to store files.
    """

    def __init__(
        self,
        *,
        run_id: str | None = None,
        base_dir: str = "./local",
    ):
        super().__init__(run_id=run_id)
        self.run_dir = os.path.join(base_dir, self.run_id)
        os.makedirs(self.run_dir, exist_ok=True)

        self.files_dir = os.path.join(self.run_dir, "files")
        self.input_dir = os.path.join(self.run_dir, "input")
        self.output_dir = os.path.join(self.run_dir, "output")
        os.makedirs(self.files_dir, exist_ok=True)
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def _idempotent_write(self, path: str, data: str):
        if os.path.exists(path):
            with open(path, "r") as f:
                assert f.read() == data
        else:
            with open(path, "x") as f:
                f.write(data)

    def get_file_path(self, path: str) -> str:
        return os.path.join(self.files_dir, path)

    @property
    def workflow_path(self) -> str:
        return os.path.join(self.run_dir, "workflow.json")

    @property
    def workflow_input_path(self) -> str:
        return os.path.join(self.run_dir, "input.json")

    @property
    def workflow_error_path(self) -> str:
        return os.path.join(self.run_dir, "error.json")

    @property
    def workflow_output_path(self) -> str:
        return os.path.join(self.run_dir, "output.json")

    def get_node_input_path(self, node_id: str) -> str:
        return os.path.join(self.input_dir, f"{node_id}.json")

    def node_error_path(self, node_id: str) -> str:
        return os.path.join(self.run_dir, f"{node_id}.error.json")

    def get_node_output_path(self, node_id: str) -> str:
        return os.path.join(self.output_dir, f"{node_id}.json")

    async def read(
        self,
        file: File,
    ) -> bytes:
        path = self.get_file_path(file.path)
        if not os.path.exists(path):
            raise UserException(f"File {file.path} not found")
        try:
            with open(path, "rb") as f:
                return f.read()
        except Exception as e:
            raise UserException(f"Failed to read file {file.path}") from e

    async def write(
        self,
        file: F,
        content: bytes,
    ) -> F:
        path = self.get_file_path(file.path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            with open(path, "wb") as f:
                f.write(content)
        except Exception as e:
            raise UserException(f"Failed to write file {file.path}") from e
        return file

    async def on_node_start(
        self,
        *,
        node: Node,
        input: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        self._idempotent_write(
            path=self.get_node_input_path(node.id),
            data=json.dumps(input),
        )

        output_path = self.get_node_output_path(node.id)
        if os.path.exists(output_path):
            with open(output_path, "r") as f:
                output = json.load(f)
            return output
        return None

    async def on_node_error(
        self,
        *,
        node: Node,
        input: Mapping[str, Any],
        exception: Exception,
    ) -> Exception | Mapping[str, Any]:
        self._idempotent_write(
            path=self.node_error_path(node.id),
            data=json.dumps(exception),
        )
        return exception

    async def on_node_finish(
        self,
        *,
        node: Node,
        input: Mapping[str, Any],
        output: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        self._idempotent_write(
            path=self.get_node_output_path(node.id),
            data=json.dumps(output),
        )
        return output

    async def on_workflow_start(
        self,
        *,
        workflow: Workflow,
        input: Mapping[str, Any],
    ) -> tuple[WorkflowErrors, Mapping[str, Any]] | None:
        """
        Triggered when a workflow is started.
        If the context already knows what the node's output will be, it can
        return the output to skip node execution.
        """
        self._idempotent_write(
            path=self.workflow_input_path,
            data=json.dumps(
                {
                    k: v.model_dump() if isinstance(v, BaseModel) else v
                    for k, v in input.items()
                }
            ),
        )

        self._idempotent_write(
            path=self.workflow_path,
            data=workflow.model_dump_json(),
        )

        output_path = self.workflow_output_path
        if os.path.exists(output_path):
            with open(output_path, "r") as f:
                output = json.load(f)
            assert isinstance(output, dict)
            return WorkflowErrors(), output

        error_path = self.workflow_error_path
        if os.path.exists(error_path):
            with open(error_path, "r") as f:
                error_and_output = json.load(f)
            assert isinstance(error_and_output, dict)
            errors = WorkflowErrors.model_validate(error_and_output["errors"])
            output = error_and_output["output"]
            assert isinstance(output, dict)
            return errors, output

        return None

    async def on_workflow_error(
        self,
        *,
        workflow: Workflow,
        input: Mapping[str, Any],
        errors: WorkflowErrors,
        partial_output: Mapping[str, Any],
    ) -> tuple[WorkflowErrors, Mapping[str, Any]]:
        self._idempotent_write(
            path=self.workflow_error_path,
            data=json.dumps(
                {
                    "errors": errors.model_dump(),
                    "output": partial_output,
                }
            ),
        )
        return errors, partial_output

    async def on_workflow_finish(
        self,
        *,
        workflow: Workflow,
        input: Mapping[str, Any],
        output: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        self._idempotent_write(
            path=self.workflow_output_path,
            data=json.dumps(output),
        )
        return output


__all__ = [
    "LocalContext",
]
