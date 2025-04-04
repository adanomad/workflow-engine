# workflow_engine/resolvers/in_memory.py
import os
import uuid
import io
import shutil
import json
import logging
import mimetypes
from typing import Any, Dict, List, Callable, Optional

from .base import BaseResolver, ResolverError
from ..types import (
    File,
    FileExecutionData,
    NodeOutputData,
    calc_file_size,
    Node,
)
from ..registry import registry

logger = logging.getLogger(__name__)


class InMemoryResolver(BaseResolver):
    """
    A resolver that stores workflow data in memory during execution
    and can optionally persist final results to the local filesystem.
    """

    def __init__(
        self, persist_results: bool = False, output_dir: str = "./workflow_output"
    ):
        """
        Initializes the InMemoryResolver.

        Args:
            persist_results: If True, save final node outputs to disk after the run.
            output_dir: The base directory to save results if persist_results is True.
        """
        # Stores results per run: {run_id: {node_id: NodeOutputData}}
        self._run_results_store: Dict[str, Dict[str, NodeOutputData]] = {}
        self._persist_results = persist_results
        self._output_dir = output_dir
        logger.info(
            f"InMemoryResolver initialized. Persist results: {self._persist_results}"
        )
        if self._persist_results:
            logger.info(f"Output directory set to: {os.path.abspath(self._output_dir)}")

    async def get_node_files(
        self, node_id: str, mime_type: str, run_id: str
    ) -> List[FileExecutionData]:
        """Retrieves files produced by a node in the current run from memory."""
        if run_id not in self._run_results_store:
            logger.debug(f"No results found for run_id: {run_id}")
            return []

        source_node_results = self._run_results_store[run_id].get(node_id, [])
        if not source_node_results:
            logger.debug(f"No results found for node {node_id} in run {run_id}")
            return []

        matching_files = [
            exec_data
            for exec_data in source_node_results
            if exec_data.metadata.file_type == mime_type
        ]
        logger.debug(
            f"Found {len(matching_files)} files for node {node_id} (mime: {mime_type}) in run {run_id}"
        )
        # Return copies to prevent downstream modification of stored data? For now, return refs.
        # Consider: return [dataclasses.replace(f) for f in matching_files] if mutation is a concern
        return matching_files

    async def get_function(self, reference_id: str) -> Callable:
        """Gets function callable from the global registry."""
        function = registry.get_function(reference_id)
        if not function:
            raise ResolverError(f"Function '{reference_id}' not found in registry")
        logger.debug(f"Retrieved function '{reference_id}' from registry.")
        return function

    async def get_function_config(self, node_data: Node) -> Dict[str, Any]:
        """Retrieves configuration directly from the Node object's config field."""
        logger.debug(f"Retrieving config for node {node_data.id} from node definition.")
        # Return a copy to prevent modification of the original workflow definition
        return (node_data.config or {}).copy()

    async def save_node_results(
        self, node_id: str, results: NodeOutputData, run_id: str
    ) -> List[File]:
        """Stores node results in memory for the current run."""
        if run_id not in self._run_results_store:
            self._run_results_store[run_id] = {}

        processed_results: NodeOutputData = []
        output_metadata: List[File] = []

        for exec_data in results:
            # Ensure metadata is consistent before storing
            metadata = exec_data.metadata
            content = exec_data.content  # Keep content reference

            if not metadata.id:
                metadata.id = uuid.uuid4().hex
            if metadata.file_size is None and content is not None:
                try:
                    metadata.file_size = calc_file_size(content)
                except Exception as e:
                    logger.warning(
                        f"Could not calculate file size for node {node_id} output '{metadata.title}': {e}"
                    )
                    metadata.file_size = -1  # Indicate unknown size
            if not metadata.title:
                metadata.title = metadata.id

            # Store a potentially updated metadata object and the original content ref
            processed_results.append(
                FileExecutionData(metadata=metadata, content=content)
            )
            # Return a copy of the final metadata state
            output_metadata.append(metadata.model_copy(deep=True))

        self._run_results_store[run_id][node_id] = processed_results
        logger.debug(
            f"Stored {len(processed_results)} results for node {node_id} in run {run_id} in memory."
        )

        return output_metadata

    async def persist_run_results(
        self, run_id: str, output_base_dir: Optional[str] = None
    ):
        """Saves the results of a completed run from memory to disk."""
        if not self._persist_results:
            logger.debug(f"Persistence disabled. Skipping disk save for run {run_id}.")
            return

        if run_id not in self._run_results_store:
            logger.warning(
                f"Cannot persist results. Run ID {run_id} not found in memory store."
            )
            return

        base_dir = output_base_dir or self._output_dir
        run_output_dir = os.path.join(base_dir, run_id)

        try:
            os.makedirs(run_output_dir, exist_ok=True)
            logger.info(f"Persisting results for run {run_id} to {run_output_dir}")

            run_data = self._run_results_store.get(run_id, {})
            for node_id, node_results in run_data.items():
                node_dir = os.path.join(run_output_dir, node_id)
                os.makedirs(node_dir, exist_ok=True)

                for exec_data in node_results:
                    metadata = exec_data.metadata
                    content = exec_data.content

                    # Create safe filename
                    safe_filename_base = "".join(
                        c if c.isalnum() or c in ("_", "-") else "_"
                        for c in metadata.title or metadata.id
                    )
                    file_ext = mimetypes.guess_extension(metadata.file_type or "") or ""
                    output_filename = (
                        f"{safe_filename_base}{file_ext}"
                        if file_ext
                        else safe_filename_base
                    )
                    output_path = os.path.join(node_dir, output_filename)
                    metadata_path = os.path.join(
                        node_dir, f"{safe_filename_base}.meta.json"
                    )

                    try:
                        logger.debug(f"Writing file content to: {output_path}")
                        if content is None:
                            logger.warning(
                                f"Skipping content write for {output_path}, content is None."
                            )
                            continue

                        if isinstance(content, bytes):
                            with open(output_path, "wb") as f:
                                f.write(content)
                        elif isinstance(content, io.IOBase):
                            try:
                                content.seek(0)  # Ensure reading from start
                                with open(output_path, "wb") as f:
                                    shutil.copyfileobj(content, f)
                                content.seek(0)  # Reset stream position if possible
                            except (io.UnsupportedOperation, AttributeError):
                                logger.warning(
                                    f"Stream for {output_path} not seekable, attempting single read."
                                )
                                # Re-read might fail if already read. Best effort.
                                try:
                                    # If it wasn't seekable, it might be exhausted.
                                    # This part is inherently fragile if streams aren't reusable.
                                    # Let's assume it needs to be read fresh if seek failed.
                                    # This relies on the content *not* being exhausted previously.
                                    # A better approach is to ensure functions return reusable content (bytes or seekable streams).
                                    with open(output_path, "wb") as f:
                                        f.write(content.read())  # Read whatever is left
                                except Exception as read_err:
                                    logger.error(
                                        f"Failed to read non-seekable stream for {output_path}: {read_err}"
                                    )
                                    continue  # Skip writing this file
                        else:
                            logger.warning(
                                f"Skipping file write for node {node_id}, unsupported content type: {type(content)}"
                            )
                            continue

                        # Save metadata as JSON sidecar
                        metadata_to_save = metadata.model_copy(
                            deep=True
                        )  # Work with a copy
                        metadata_to_save.local_path = os.path.abspath(
                            output_path
                        )  # Add local path info
                        logger.debug(f"Writing file metadata to: {metadata_path}")
                        with open(metadata_path, "w") as f:
                            json.dump(
                                metadata_to_save.model_dump(mode="json"), f, indent=2
                            )

                    except IOError as e:
                        logger.error(
                            f"Error writing file {output_path} for node {node_id}: {e}",
                            exc_info=True,
                        )
                    except Exception as e:
                        logger.error(
                            f"Unexpected error saving file {output_path} or metadata: {e}",
                            exc_info=True,
                        )

            logger.info(
                f"Successfully persisted results for run {run_id} to {run_output_dir}"
            )

        except OSError as e:
            logger.error(
                f"Failed to create output directories for run {run_id} under {base_dir}: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during result persistence for run {run_id}: {e}",
                exc_info=True,
            )

    def clear_run_data(self, run_id: str):
        """Removes data for a specific run from the memory store."""
        if run_id in self._run_results_store:
            # Explicitly help GC by clearing content references if they are large?
            # for node_results in self._run_results_store[run_id].values():
            #     for exec_data in node_results:
            #         exec_data.content = None # Or del exec_data.content if possible
            del self._run_results_store[run_id]
            logger.info(f"Cleared in-memory data for run {run_id}")
