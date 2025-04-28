# workflow_engine/nodes/text.py
import os
from typing import Literal

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
        filename, ext = os.path.splitext(input.file.path)
        new_file = TextFile(path=filename + self.params.suffix + ext)
        new_file = new_file.write_text(context, text=new_text)
        return AppendToFileOutput(file=new_file)


__all__ = [
    "AppendToFileNode",
    "AppendToFileParams",
]
