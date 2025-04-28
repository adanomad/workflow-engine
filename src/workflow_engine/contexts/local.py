# workflow_engine/contexts/local.py
import json
import os
from typing import Any, Mapping, TypeVar

from pydantic import BaseModel

from ..core import Context, Data, File, Node, Workflow


F = TypeVar("F", bound=File)


class LocalContext(Context):
    """
    A context that uses the local filesystem to store files.
    """
    def __init__(
            self,
            run_id: str,
            *,
            base_dir: str = "./local",
    ):
        super().__init__(run_id)
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
    def workflow_output_path(self) -> str:
        return os.path.join(self.run_dir, "output.json")

    def get_node_input_path(self, node_id: str) -> str:
        return os.path.join(self.input_dir, f"{node_id}.json")

    def get_node_output_path(self, node_id: str) -> str:
        return os.path.join(self.output_dir, f"{node_id}.json")

    def read(
            self,
            file: File,
    ) -> bytes:
        path = self.get_file_path(file.path)
        with open(path, "rb") as f:
            return f.read()

    def write(
            self,
            file: F,
            content: bytes,
    ) -> F:
        path = self.get_file_path(file.path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(content)
        return file

    def on_workflow_start(
            self,
            *,
            workflow: Workflow,
            input: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """
        Triggered when a workflow is started.
        If the context already knows what the node's output will be, it can
        return the output to skip node execution.
        """
        self._idempotent_write(
            path=self.workflow_input_path,
            data=json.dumps({
                k: v.model_dump() if isinstance(v, BaseModel) else v
                for k, v in input.items()
            }),
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
            return output
        return None

    def on_node_start(
            self,
            *,
            node: Node,
            input: Data,
    ) -> Data | None:
        self._idempotent_write(
            path=self.get_node_input_path(node.id),
            data=input.model_dump_json(),
        )

        output_path = self.get_node_output_path(node.id)
        if os.path.exists(output_path):
            with open(output_path, "r") as f:
                output = node.output_type.model_validate_json(f.read()) # type: ignore
            return output
        return None

    def on_node_finish(
            self,
            *,
            node: Node,
            input: Data,
            output: Data,
    ) -> Data:
        self._idempotent_write(
            path=self.get_node_output_path(node.id),
            data=output.model_dump_json(),
        )
        return output

    def on_workflow_finish(
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
