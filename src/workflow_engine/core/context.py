# workflow_engine/core/context.py
from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Mapping

from .data import Data
from .file import File
if TYPE_CHECKING:
    from .node import Node
    from .workflow import Workflow


class Context(ABC):
    """
    Represents the environment in which a workflow is executed.
    A context's life is limited to the execution of a single workflow.
    """
    def __init__(
            self,
            run_id: str,
    ):
        self.run_id = run_id

    @abstractmethod
    def read(
            self,
            file: File,
    ) -> bytes:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def write(
            self,
            file: File,
            content: bytes,
    ) -> None:
        raise NotImplementedError("Subclasses must implement this method")

    def on_workflow_start(
            self,
            *,
            workflow: "Workflow",
            input: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """
        A hook that is called when a workflow starts execution.

        If the context already knows what the workflow's output will be, return
        that output to skip workflow execution.
        """
        pass

    def on_node_start(
            self,
            *,
            node: "Node",
            input: Data,
    ) -> Data | None:
        """
        A hook that is called when a node starts execution.

        If the context already knows what the node's output will be, return that
        output to skip node execution.
        """
        pass

    def on_node_finish(
            self,
            *,
            node: "Node",
            input: Data,
            output: Data,
    ) -> None:
        """
        A hook that is called when a node finishes execution.
        """
        pass

    def on_workflow_finish(
            self,
            *,
            workflow: "Workflow",
            input: Mapping[str, Any],
            output: Mapping[str, Any],
    ) -> None:
        """
        A hook that is called when a workflow finishes execution.
        """
        pass


__all__ = [
    "Context",
]
