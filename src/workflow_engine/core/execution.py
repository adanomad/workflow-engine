# workflow_engine/core/execution.py
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from .context import Context
from .error import WorkflowErrors
from .workflow import Workflow


class ExecutionAlgorithm(ABC):
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
        input: Mapping[str, Any],
    ) -> tuple[WorkflowErrors, Mapping[str, Any]]:
        pass


__all__ = [
    "ExecutionAlgorithm",
]
