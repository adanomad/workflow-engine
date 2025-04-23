# workflow_engine/nodes/text.py
from typing import Any, Literal

from ..core import Context, Data, JSONFile, Node, Params, TextFile


class AppendToFileInput(Data):
    file: TextFile
    text: str

class AppendToFileOutput(Data):
    file: TextFile

class AppendToFileParams(Params):
    suffix: str

class AppendToFileNode(Node[AppendToFileInput, AppendToFileOutput, AppendToFileParams]):
    type: Literal["AppendToFile"] = "AppendToFile"

    @property
    def input_type(self):
        return AppendToFileInput

    @property
    def output_type(self):
        return AppendToFileOutput

    def __call__(self, context: Context, input: AppendToFileInput) -> AppendToFileOutput:
        old_text = input.file.read_text(context)
        new_text = old_text + input.text
        new_file = TextFile(path=input.file.path + self.params.suffix)
        new_file.write_text(context, text=new_text)
        return AppendToFileOutput(file=new_file)


class DumpJSONInput(Data):
    data: Any

class DumpJSONOutput(Data):
    file: JSONFile

class DumpJSONParams(Params):
    file_name: str
    indent: int = 0 # default: no indentation

class DumpJSONNode(Node[DumpJSONInput, DumpJSONOutput, DumpJSONParams]):
    """
    Saves its input as a JSON file.
    """
    type: Literal["DumpJSON"] = "DumpJSON"

    @property
    def input_type(self):
        return DumpJSONInput

    @property
    def output_type(self):
        return DumpJSONOutput

    def __call__(self, context: Context, input: DumpJSONInput) -> DumpJSONOutput:
        file = JSONFile(path=self.params.file_name)
        file.write_data(context, input.data)
        return DumpJSONOutput(file=file)


__all__ = [
    "AppendToFileNode",
    "AppendToFileParams",
    "DumpJSONNode",
    "DumpJSONParams",
]
