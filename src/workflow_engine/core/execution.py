# workflow_engine/core/execution.py
from abc import ABC, abstractmethod

from overrides import EnforceOverrides

from .context import Context
from .data import DataMapping
from .error import WorkflowErrors
from .workflow import Workflow


class ExecutionAlgorithm(ABC, EnforceOverrides):
    """
    Handles the scheduling and execution of workflow nodes.
    Uses hooks to perform extra functionality at key points in the execution
    flow.
    """

    @abstractmethod
    async def execute(
        self,
        *,
        context: Context,
        workflow: Workflow,
        input: DataMapping,
    ) -> tuple[WorkflowErrors, DataMapping]:
        pass


__all__ = [
    "ExecutionAlgorithm",
]
