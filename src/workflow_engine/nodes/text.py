# workflow_engine/nodes/text.py
import os
from typing import Literal, Self

from ..core import (
    Context,
    Data,
    File,
    Node,
    Params,
    StringValue,
)
from ..files import TextFileValue


class AppendToFileInput(Data):
    file: TextFileValue
    text: StringValue


class AppendToFileOutput(Data):
    file: TextFileValue


class AppendToFileParams(Params):
    suffix: StringValue


class AppendToFileNode(Node[AppendToFileInput, AppendToFileOutput, AppendToFileParams]):
    type: Literal["AppendToFile"] = "AppendToFile"

    @property
    def input_type(self):
        return AppendToFileInput

    @property
    def output_type(self):
        return AppendToFileOutput

    async def run(
        self,
        context: Context,
        input: AppendToFileInput,
    ) -> AppendToFileOutput:
        old_text = await input.file.read_text(context)
        new_text = old_text + input.text.root
        filename, ext = os.path.splitext(input.file.root.path)
        new_file = TextFileValue(File(path=filename + self.params.suffix.root + ext))
        new_file = await new_file.write_text(context, text=new_text)
        return AppendToFileOutput(file=new_file)

    @classmethod
    def from_suffix(cls, id: str, suffix: str) -> Self:
        return cls(id=id, params=AppendToFileParams(suffix=StringValue(suffix)))


__all__ = [
    "AppendToFileNode",
]
