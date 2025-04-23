import pytest

from pydantic import ValidationError

from workflow_engine.core import Data, Edge, Params
from workflow_engine.nodes import (
    AppendToFileNode,
    AppendToFileParams,
    ConstantBool,
    ConstantBoolNode,
    DumpJSONNode,
    DumpJSONParams,
)


@pytest.mark.unit
def test_default_parameters():
    params = DumpJSONParams.model_validate({
        "file_name": "dump.json",
    })
    assert params.indent == 0


@pytest.mark.unit
def test_extra_parameters():
    # Params is the only type that can have extra parameters
    params = Params.model_validate({
        "something": "else",
    })
    assert params.model_extra == {"something": "else"}

    # these will not successfully deserialize due to extra parameters
    with pytest.raises(ValidationError):
        Data.model_validate({
            "something": "else",
        })
    with pytest.raises(ValidationError):
        ConstantBool.model_validate({
            "value": True,
            "something": "else",
        })
    with pytest.raises(ValidationError):
        ConstantBoolNode.model_validate({
            "id": "1",
            "params": {"value": True},
            "something": "else",
        })


@pytest.mark.unit
def test_edge_validation():
    dump = DumpJSONNode(id="1", params=DumpJSONParams(file_name="dump.json"))
    append = AppendToFileNode(id="2", params=AppendToFileParams(suffix=".json"))

    # validate that a JSON file can be passed as a Text file
    Edge.from_nodes(
        source=dump,
        target=append,
        source_key="file",
        target_key="file",
    )

    # validate that a Text file can technically be passed as Any (why would you do this?)
    Edge.from_nodes(
        source=append,
        target=dump,
        source_key="file",
        target_key="data",
    )
