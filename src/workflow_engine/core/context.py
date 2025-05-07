# workflow_engine/core/context.py
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, TypeVar

from .file import File
from .error import WorkflowErrors
from .node import Node
from .workflow import Workflow


F = TypeVar("F", bound=File)


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
        file: F,
        content: bytes,
    ) -> F:
        raise NotImplementedError("Subclasses must implement this method")

    def on_node_start(
        self,
        *,
        node: "Node",
        input: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """
        A hook that is called when a node starts execution.

        If the context already knows what the node's output will be, return that
        output to skip node execution.
        """
        return None

    def on_node_error(
        self,
        *,
        node: "Node",
        input: Mapping[str, Any],
        exception: Exception,
    ) -> Exception | Mapping[str, Any]:
        """
        A hook that is called when a node raises an error.
        The context can modify the error by returning a different Exception, or
        it can silence the error by returning an output.
        """
        return exception

    def on_node_finish(
        self,
        *,
        node: "Node",
        input: Mapping[str, Any],
        output: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        A hook that is called when a node finishes execution.
        """
        return output

    def on_workflow_start(
        self,
        *,
        workflow: "Workflow",
        input: Mapping[str, Any],
    ) -> tuple[WorkflowErrors, Mapping[str, Any]] | None:
        """
        A hook that is called when a workflow starts execution.

        If the context already knows what the workflow's output will be, return
        that output to skip workflow execution.
        """
        return None

    def on_workflow_error(
        self,
        *,
        workflow: "Workflow",
        input: Mapping[str, Any],
        errors: WorkflowErrors,
        partial_output: Mapping[str, Any],
    ) -> tuple[WorkflowErrors, Mapping[str, Any]]:
        """
        A hook that is called when a workflow raises an error.
        The context can modify the errors or partial output by returning a
        different tuple.
        """
        return errors, partial_output

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
        return output


__all__ = [
    "Context",
]
