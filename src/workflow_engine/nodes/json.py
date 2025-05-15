# workflow_engine/nodes/json.py
from collections.abc import Sequence
from typing import Any, Literal

from ..core import (
    Context,
    Data,
    Empty,
    Node,
    Params,
)
from ..files import JSONFile, JSONLinesFile


class JSONData(Data):
    data: Any


class JSONFileData(Data):
    file: JSONFile


class ReadJSONNode(Node[JSONFileData, JSONData, Empty]):
    """
    Reads a JSON file into a JSON object.
    """

    type: Literal["ReadJSON"] = "ReadJSON"

    @property
    def input_type(self):
        return JSONFileData

    @property
    def output_type(self):
        return JSONData

    async def run(self, context: Context, input: JSONFileData) -> JSONData:
        return JSONData(data=await input.file.read_data(context))


class WriteJSONParams(Params):
    file_name: str
    indent: int = 0  # default: no indentation


class WriteJSONNode(Node[JSONData, JSONFileData, WriteJSONParams]):
    """
    Saves its input as a JSON file.
    """

    type: Literal["WriteJSON"] = "WriteJSON"

    @property
    def input_type(self):
        return JSONData

    @property
    def output_type(self):
        return JSONFileData

    async def run(self, context: Context, input: JSONData) -> JSONFileData:
        file = JSONFile(path=self.params.file_name)
        file = await file.write_data(context, input.data)
        return JSONFileData(file=file)


class JSONLinesData(Data):
    data: Sequence[Any]


class JSONLinesFileData(Data):
    file: JSONLinesFile


class ReadJSONLinesNode(Node[JSONLinesFileData, JSONLinesData, Empty]):
    """
    Reads a JSONLines file into a list of Any.
    """

    type: Literal["ReadJSONLines"] = "ReadJSONLines"

    @property
    def input_type(self):
        return JSONLinesFileData

    @property
    def output_type(self):
        return JSONLinesData

    async def run(self, context: Context, input: JSONLinesFileData) -> JSONLinesData:
        return JSONLinesData(data=await input.file.read_data(context))


class WriteJSONLinesParams(Params):
    file_name: str
    # no indent allowed for JSON lines


class WriteJSONLinesNode(Node[JSONLinesData, JSONLinesFileData, WriteJSONLinesParams]):
    """
    Saves its input as a JSONLines file.
    """

    type: Literal["WriteJSONLines"] = "WriteJSONLines"

    @property
    def input_type(self):
        return JSONLinesData

    @property
    def output_type(self):
        return JSONLinesFileData

    async def run(self, context: Context, input: JSONLinesData) -> JSONLinesFileData:
        file = JSONLinesFile(path=self.params.file_name)
        file = await file.write_data(context, input.data)
        return JSONLinesFileData(file=file)


__all__ = [
    "ReadJSONNode",
    "ReadJSONLinesNode",
    "WriteJSONNode",
    "WriteJSONLinesNode",
    "WriteJSONParams",
    "WriteJSONLinesParams",
]
