# workflow_engine/resolvers/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Callable, Optional
from ..types import File, FileExecutionData, NodeOutputData, Node


class ResolverError(Exception):
    """Custom exception for resolver-related errors."""

    pass


class BaseResolver(ABC):
    """Abstract base class defining the interface for workflow resolvers."""

    @abstractmethod
    async def get_node_files(
        self, node_id: str, mime_type: str, run_id: str
    ) -> List[FileExecutionData]:
        """
        Gets file metadata and content produced by a source node for a specific run.
        """
        pass

    @abstractmethod
    async def get_function(self, reference_id: str) -> Callable:
        """
        Retrieves the callable function implementation using its reference ID.
        """
        pass

    @abstractmethod
    async def get_function_config(self, node_data: Node) -> Dict[str, Any]:
        """
        Retrieves the specific configuration parameters for a given node instance.
        """
        pass

    @abstractmethod
    async def save_node_results(
        self, node_id: str, results: NodeOutputData, run_id: str
    ) -> List[File]:
        """
        Saves the results (files) produced by a node execution for a specific run.
        Returns the final metadata of the saved files.
        """
        pass

    async def initialize(self):
        """Initialize the resolver if necessary (e.g., database connections)."""
        pass

    async def persist_run_results(
        self, run_id: str, output_base_dir: Optional[str] = None
    ):
        """Optionally persists results stored by the resolver after a run."""
        pass
