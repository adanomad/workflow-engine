# my_workflow_engine/types.py
import os
import uuid
from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any, List, Optional, Union, IO
from dataclasses import dataclass


class Node(BaseModel):
    id: str
    reference_id: str  # str_id of tool in registry/db


class Edge(BaseModel):
    id: str
    source_node_id: str = Field(..., alias="source")
    target_node_id: str = Field(..., alias="target")
    sourceHandle: str  # Mime type expected by the source parameter
    targetHandle: str  # Mime type expected by the target parameter
    target_parameter: str  # Name of the parameter in the target node function


class WorkflowGraph(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

    @field_validator("edges")
    def check_edge_nodes_exist(cls, edges, info):
        node_ids = {node.id for node in info.data.get("nodes", [])}
        for edge in edges:
            if edge.source_node_id not in node_ids:
                raise ValueError(
                    f"Edge source node '{edge.source_node_id}' not found in nodes"
                )
            if edge.target_node_id not in node_ids:
                raise ValueError(
                    f"Edge target node '{edge.target_node_id}' not found in nodes"
                )
        return edges


Json = Dict[str, Any]


class File(BaseModel):
    id: str  # = Field(default_factory=lambda: uuid.uuid4().hex)
    user: Optional[str] = None
    title: Optional[str] = None
    file_type: Optional[str] = None  # Mime type
    file_size: Optional[int] = None
    chunk_size: Optional[int] = None
    created_at: Optional[str] = None
    delete_on: Optional[str] = None
    file_id: Optional[str] = None
    metadata: Optional[Json] = Field(default_factory=dict)
    local_path: Optional[str] = None


@dataclass
class FileExecutionData:
    metadata: File  # The metadata object matching the DB row
    content: Union[bytes, IO[bytes]]  # The actual downloaded file content


# Key: mime_type, Value: list of files with that ty[e]
NodeInputData = Dict[str, Union[List[FileExecutionData], Any]]

# Key: mime_type, Value: list of files with that ty[e]
NodeOutputData = TypeAlias = List[FileExecutionData]

# Key: node_id, Value: NodeOutputData returned by that node
WorkflowRunResults = Dict[str, List[File]]


def calc_file_size(file_content: Union[bytes, IO[bytes]]) -> int:
    """Calculates the size of file content (bytes or file-like object)."""
    if isinstance(file_content, bytes):
        return len(file_content)
    elif hasattr(file_content, "seek") and hasattr(file_content, "tell"):
        current_pos = file_content.tell()
        file_content.seek(0, os.SEEK_END)
        size = file_content.tell()
        file_content.seek(current_pos)  # Reset position
        return size
    else:
        raise TypeError("Unsupported type for file size calculation.")
