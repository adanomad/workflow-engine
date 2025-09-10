from hashlib import md5

import pytest

from workflow_engine import (
    Context,
    File,
    IntegerValue,
    SequenceValue,
    StringMapValue,
)
from workflow_engine.contexts.in_memory import InMemoryContext
from workflow_engine.files import JSONFileValue, JSONLinesFileValue


@pytest.fixture
def context():
    """Create a test context for value casting operations."""
    return InMemoryContext()


@pytest.mark.unit
async def test_cast_jsonlines_to_sequence(context: Context):
    """Test that JSONLinesFileValue can be cast to a SequenceValue."""
    jsonl_file = JSONLinesFileValue.from_path("input.jsonl")
    contents = [{"a": 1}, {"b": 2}, {"c": 3}]
    contents_str = '{"a": 1}\n{"b": 2}\n{"c": 3}'

    await jsonl_file.write_data(context, contents)

    assert (await jsonl_file.read_text(context)) == contents_str

    data = await SequenceValue[StringMapValue[IntegerValue]].cast_from(
        jsonl_file,
        context=context,
    )
    assert data == contents

    json_files = await SequenceValue[JSONFileValue].cast_from(
        jsonl_file,
        context=context,
    )
    assert json_files == SequenceValue[JSONFileValue](
        # md5 hashes of the data
        [
            JSONFileValue(File(path=f"{md5(b"{'a': 1}").hexdigest()}.json")),
            JSONFileValue(File(path=f"{md5(b"{'b': 2}").hexdigest()}.json")),
            JSONFileValue(File(path=f"{md5(b"{'c': 3}").hexdigest()}.json")),
        ]
    )
    assert (await json_files[0].read_data(context)) == {"a": 1}
    assert (await json_files[1].read_data(context)) == {"b": 2}
    assert (await json_files[2].read_data(context)) == {"c": 3}
