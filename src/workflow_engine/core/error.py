# workflow_engine/core/error.py

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict


class NodeExecutionError(BaseModel):
    """
    Any error message representing a problem that prevents the node from
    producing results.
    Note that this is not an exception class, and should not actually be raised.
    Instead, nodes should "raise" this exception by returning this object.

    Exceptions raised by nodes will not be handled gracefully.

    ```
    try:
        ...
    except SomeException as e:
        return NodeExecutionError(...)
    ```
    """

    model_config = ConfigDict(frozen=True)

    node_id: str
    message: str


class WorkflowExecutionError(BaseModel):
    """
    Any error message representing a problem that prevents the workflow from
    producing full results.
    """

    model_config = ConfigDict(frozen=True)

    node_errors: Mapping[str, NodeExecutionError]
    partial_output: Mapping[str, Any]


__all__ = [
    "NodeExecutionError",
    "WorkflowExecutionError",
]
