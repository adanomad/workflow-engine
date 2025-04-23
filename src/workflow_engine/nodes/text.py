# workflow_engine/nodes/text.py
from typing import Any, Literal, Sequence

from ..core import (
    Context,
    Data,
    Node,
    Params,
    TextFile,
)


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


__all__ = [
    "AppendToFileNode",
    "AppendToFileParams",
]
