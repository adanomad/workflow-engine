from abc import ABC, abstractmethod
from typing import Any, Mapping

from .context import Context
from .workflow import Workflow


class ExecutionAlgorithm(ABC):
    """
    Handles the scheduling and execution of workflow nodes.
    Uses hooks to perform extra functionality at key points in the execution
    flow.
    """
    @abstractmethod
    def execute(
            self,
            *,
            context: Context,
            workflow: Workflow,
            input: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        pass


__all__ = [
    "ExecutionAlgorithm",
]
