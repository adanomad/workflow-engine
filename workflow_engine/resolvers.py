# my_workflow_engine/resolvers.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Callable, Union, Optional
from supabase import create_async_client, AsyncClient
import os
from .types import File, FileExecutionData, NodeOutputData, calc_file_size
from .registry import registry
import logging
import io

logger = logging.getLogger(__name__)


class ResolverError(Exception):
    pass


class BaseResolver(ABC):
    @abstractmethod
    async def get_node_files(
        self, node_id: str, mime_type: str
    ) -> List[FileExecutionData]:
        """
        Gets file metadata produced by source_node_id matching mime_type,
        downloads the content, and returns combined execution data.
        """
        pass

    @abstractmethod
    async def get_function(self, reference_id: str) -> Callable:
        """
        Retrieves the callable function implementation using its reference ID.
        """
        pass

    @abstractmethod
    async def get_function_config(self, node_id: str) -> Dict[str, Any]:
        """
        Retrieves the specific configuration parameters for a given node instance
        in the workflow.
        Args:
            node_id: The unique ID of the node instance in the workflow graph.
        Returns:
            Dict[str, Any]: Dictionary of configured parameter names and their values.
        """
        pass

    @abstractmethod
    async def save_node_results(
        self, node_id: str, results: NodeOutputData
    ) -> List[File]:
        """
        Save the results of a node execution
        """
        pass


class SupabaseResolver(BaseResolver):
    def __init__(
        self, user_id: str, workflow_id: str, storage_bucket: str = "documents_storage"
    ):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY environment variables must be set"
            )

        if not user_id:
            raise ValueError("user_id must be provided")

        if not workflow_id:
            raise ValueError("workflow_id must be provided")

        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.supabase: Optional[AsyncClient] = None
        self.user_id = user_id
        self.workflow_id = workflow_id
        self.storage_bucket = storage_bucket

    async def initialize(self):
        """Initialize the Supabase client asynchronously"""
        if self.supabase is None:
            self.supabase = await create_async_client(
                self.supabase_url, self.supabase_key
            )
        return self

    async def get_node_files(
        self, node_id: str, mime_type: str
    ) -> List[FileExecutionData]:
        """Fetches file metadata and downloads content for files matching node_id and mime_type."""
        try:
            response = await (
                self.supabase.table("document_info")
                .select("*, node_workspace_docs!inner(node_ref_id)")
                .eq("node_workspace_docs.node_ref_id", node_id)
                .eq("file_type", mime_type)
                .execute()
            )

            if not response.data:
                return []

            execution_data_list: List[FileExecutionData] = []
            for row in response.data:
                row.pop("node_workspace_docs", None)

                try:
                    file_meta = File(**row)
                    if not file_meta.id or not file_meta.user:  # Check for user ID too
                        logger.warning(
                            f"Skipping file metadata due to missing id or user: {row}"
                        )
                        continue

                    storage_path = f"{file_meta.user}/{file_meta.id}"
                    content = await self.supabase.storage.from_(
                        self.storage_bucket
                    ).download(storage_path)

                    execution_data_list.append(
                        FileExecutionData(metadata=file_meta, content=content)
                    )

                except Exception as e:
                    raise ResolverError(f"Failed to process file: {str(e)}") from e

            return execution_data_list

        except Exception as e:
            if isinstance(e, ResolverError):
                raise e
            raise ResolverError(f"Failed to get files for node {node_id}") from e

    async def get_function(self, reference_id: str) -> Callable:
        """
        Gets function callable from registry based on str_id fetched from Supabase 'tools' table.
        """
        function_name = reference_id

        function = registry.get_function(function_name)
        if not function:
            raise ValueError(f"Function {function_name} not found in registry")

        return function

    async def get_function_config(self, node_id: str) -> Dict[str, Any]:
        """
        Gets the 'config' JSON blob from the 'node_workspaces' table for a specific node instance.
        """
        try:
            response = await (
                self.supabase.table("node_workspaces")
                .select("config")
                .eq("node_id_ref", node_id)
                .maybe_single()
                .execute()
            )

            if response and response.data and response.data.get("config"):
                config = response.data["config"]
                if isinstance(config, dict):
                    return config
                else:
                    logger.info(
                        f"Config for node {node_id} is not a valid JSON object: {config}"
                    )
                    return {}
            else:
                logger.info(
                    f"No configuration found for node {node_id}, returning empty config."
                )
                return {}

        except Exception as e:
            raise Exception(
                f"Failed to fetch function config for node {node_id}: {str(e)}"
            )

    # add VERSION number later
    async def save_node_results(
        self, node_id: str, results: NodeOutputData
    ) -> List[File]:

        final_metadata_list: List[File] = []
        for exec_data in results:
            try:
                # _save_single_file now returns the final File metadata object
                final_metadata = await self._save_single_file(node_id, exec_data)
                final_metadata_list.append(final_metadata)
            except Exception as e:
                logger.error(
                    f"Halting save process for node {node_id} due to error saving file."
                )
                raise

        return final_metadata_list

    async def _save_single_file(
        self, node_id: str, exec_data: FileExecutionData
    ) -> File:
        metadata = exec_data.metadata
        file_content = exec_data.content

        if file_content is None:
            raise ValueError(
                f"FileExecutionData is missing content for upload (metadata title: {metadata.title})."
            )
        if not metadata.file_type:
            raise ValueError(
                f"FileExecutionData metadata is missing file_type (metadata title: {metadata.title})."
            )

        file_name = metadata.title
        file_type = metadata.file_type
        file_size = metadata.file_size or calc_file_size(file_content)

        try:
            # 1. Insert document info
            insert_response = (
                await self.supabase.table("document_info")
                .insert(
                    {
                        "user": self.user_id,
                        "title": file_name,
                        "file_type": file_type,
                        "file_size": file_size,
                    }
                )
                .execute()
            )

            if not insert_response.data:
                error_details = getattr(insert_response, "error", "Unknown error")
                logger.error(f"Failed to insert document info: {error_details}")
                raise ResolverError(f"Failed to insert document info: {error_details}")

            doc_info = insert_response.data[0]
            final_file_metadata = File(**doc_info)

            # 2. Upload file to Storage Bucket (using exec_data.content)
            storage_path = f"{final_file_metadata.user}/{final_file_metadata.id}"
            logger.info(f"Uploading file to storage path: {storage_path}")

            upload_content: Union[bytes, io.BytesIO]
            if isinstance(file_content, bytes):
                upload_content = file_content
            elif isinstance(file_content, io.IOBase):
                if not file_content.seekable():
                    file_content.seek(0)
                    upload_content = io.BytesIO(file_content.read())
                else:
                    file_content.seek(0)
                    upload_content = file_content
            else:
                raise TypeError(f"Unsupported type for content: {type(file_content)}")

            try:
                await self.supabase.storage.from_(self.storage_bucket).upload(
                    path=storage_path,
                    file=upload_content,
                    file_options={"content_type": file_type, "upsert": "true"},
                )
                logger.info(f"Storage upload successful for path: {storage_path}")
            except Exception as upload_error:
                raise Exception(f"Storage upload failed: {str(upload_error)}")

            # 3. Link file to node workspace
            insert_data = [{"node_ref_id": node_id, "file_id": final_file_metadata.id}]
            workspace_result = (
                await self.supabase.table("node_workspace_docs")
                .insert(insert_data)
                .execute()
            )

            return final_file_metadata

        except Exception as error:
            logger.error(
                f"Error saving file '{file_name}' for node {node_id}: {error}",
                exc_info=True,
            )
