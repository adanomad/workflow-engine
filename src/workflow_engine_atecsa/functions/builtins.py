# my_workflow_engine/functions/builtins.py
from typing import List
from ..types import File, FileExecutionData, NodeOutputData
from ..registry import registry
import uuid
import json


# Test "input" node
@registry.register(
    name="generate_text_file",
    description="Generates a simple text file with provided content.",
)
def generate_text_file(
    content: str = "test file",
    base_name: str = "output",
    line_count: int = 1,
) -> NodeOutputData:
    """
    Generates one or more text files.
    """
    output_files: NodeOutputData = []

    full_content = "\n".join([content] * line_count)
    content_bytes = full_content.encode("utf-8")

    # Prepare metadata for the output file
    # The resolver will assign the final ID, user, created_at etc. upon saving.
    output_metadata = File(
        id=str(uuid.uuid4()),  # Temporary ID for execution context
        user="temp_user",  # Placeholder, resolver will use its own
        title=f"{base_name}.txt",
        file_type="text/plain",  # Mime type
        metadata={"generator": "generate_text_file", "lines": line_count},
    )

    exec_data = FileExecutionData(metadata=output_metadata, content=content_bytes)
    output_files.append(exec_data)

    return output_files


@registry.register(
    name="process_text_to_json",
    description="Converts lines in text files to a JSON list.",
)
def process_text_to_json(
    input_files: List[FileExecutionData], output_base_name: str = "processed"
) -> NodeOutputData:  # Returns List[FileExecutionData]
    """
    Reads input text files, treats each line as an item, and outputs a JSON file
    containing a list of these items for each input file.
    """
    output_files: NodeOutputData = []

    for i, exec_data in enumerate(input_files):
        input_metadata = exec_data.metadata
        input_content = exec_data.content

        if isinstance(input_content, bytes):
            text_content = input_content.decode("utf-8")
        elif hasattr(input_content, "read"):
            input_content.seek(0)
            text_content = input_content.read().decode("utf-8")

        lines = [line.strip() for line in text_content.splitlines() if line.strip()]
        json_output_dict = {"source_file_id": input_metadata.id, "lines": lines}
        json_content_bytes = json.dumps(json_output_dict, indent=2).encode("utf-8")

        output_metadata = File(
            id=str(uuid.uuid4()),
            user="temp_user",
            title=f"{output_base_name}_{i}.json",
            file_type="application/json",
            metadata={
                "processor": "process_text_to_json",
                "original_file_id": input_metadata.id,
                "original_title": input_metadata.title,
            },
        )

        output_exec_data = FileExecutionData(
            metadata=output_metadata, content=json_content_bytes
        )
        output_files.append(output_exec_data)

    return output_files


@registry.register(
    name="analyze_json_data",
    description="Analyzes data from JSON files and produces a summary report.",
)
def analyze_json_data(
    json_inputs: List[FileExecutionData],
    report_title: str = "Analysis Report",
    min_lines_threshold: int = 0,
) -> NodeOutputData:
    """
    Reads JSON files (expected format from process_text_to_json), counts lines,
    and generates a single text report summarizing the analysis.
    """
    report_content = f"--- {report_title} ---\n\n"
    report_content += f"Analysis based on {len(json_inputs)} input JSON file(s).\n"
    report_content += f"Minimum lines threshold: {min_lines_threshold}\n\n"
    total_lines = 0
    files_meeting_threshold = 0

    for exec_data in json_inputs:
        metadata = exec_data.metadata
        content = exec_data.content

        if isinstance(content, bytes):
            data = json.loads(content.decode("utf-8"))
        elif hasattr(content, "read"):
            content.seek(0)
            data = json.load(content)

        line_count = len(data.get("lines", []))
        total_lines += line_count
        meets_threshold = line_count >= min_lines_threshold
        if meets_threshold:
            files_meeting_threshold += 1

        report_content += f"File: {metadata.title or metadata.id}\n"
        report_content += f"  Source File ID: {data.get('source_file_id', 'N/A')}\n"
        report_content += f"  Line Count: {line_count}\n"
        report_content += f"  Meets Threshold ({min_lines_threshold}): {'Yes' if meets_threshold else 'No'}\n\n"

    report_content += f"--- Summary ---\n"
    report_content += f"Total lines across all files: {total_lines}\n"
    report_content += f"Files meeting threshold: {files_meeting_threshold}\n"

    report_metadata = File(
        id=str(uuid.uuid4()),
        user="temp_user",
        title=f"{report_title.replace(' ', '_')}.txt",
        file_type="text/plain",
        metadata={
            "analyzer": "analyze_json_data",
            "input_file_count": len(json_inputs),
            "total_lines_analyzed": total_lines,
            "threshold": min_lines_threshold,
        },
    )

    report_exec_data = FileExecutionData(
        metadata=report_metadata, content=report_content.encode("utf-8")
    )

    return [report_exec_data]
