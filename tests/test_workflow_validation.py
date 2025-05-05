# tests/test_workflow_validation.py
import pytest
from pydantic import ValidationError

from workflow_engine.core import Data, Edge, Params
from workflow_engine.nodes import (
    AppendToFileNode,
    AppendToFileParams,
    ConstantBool,
    ConstantBoolNode,
    FactorizationNode,
    ReadJSONLinesNode,
    SumNode,
    WriteJSONLinesNode,
    WriteJSONLinesParams,
    WriteJSONNode,
    WriteJSONParams,
)


@pytest.mark.unit
def test_default_parameters():
    params = WriteJSONParams.model_validate(
        {
            "file_name": "out.json",
        }
    )
    assert params.indent == 0


@pytest.mark.unit
def test_extra_parameters():
    # Params is the only type that can have extra parameters
    params = Params.model_validate(
        {
            "something": "else",
        }
    )
    assert params.model_extra == {"something": "else"}

    # these will not successfully deserialize due to extra parameters
    with pytest.raises(ValidationError):
        Data.model_validate(
            {
                "something": "else",
            }
        )
    with pytest.raises(ValidationError):
        ConstantBool.model_validate(
            {
                "value": True,
                "something": "else",
            }
        )
    with pytest.raises(ValidationError):
        ConstantBoolNode.model_validate(
            {
                "id": "1",
                "params": {"value": True},
                "something": "else",
            }
        )


@pytest.mark.unit
def test_edge_validation():
    write_node = WriteJSONNode(id="write", params=WriteJSONParams(file_name="out.json"))
    append_node = AppendToFileNode(
        id="append", params=AppendToFileParams(suffix=".json")
    )

    # validate that a JSON file can be passed as a Text file
    Edge.from_nodes(
        source=write_node,
        target=append_node,
        source_key="file",
        target_key="file",
    )

    # validate that a Text file can technically be passed as Any (why would you do this?)
    Edge.from_nodes(
        source=append_node,
        target=write_node,
        source_key="file",
        target_key="data",
    )


@pytest.mark.unit
def test_generic_edge_validation():
    factorization_node = FactorizationNode(id="factorization")
    sum_node = SumNode(id="sum")
    read_node = ReadJSONLinesNode(id="read")
    write_node = WriteJSONLinesNode(
        id="write", params=WriteJSONLinesParams(file_name="out.json")
    )

    # list[int] -> list[int]
    Edge.from_nodes(
        source=factorization_node,
        target=sum_node,
        source_key="factors",
        target_key="values",
    )

    # list[int] -> list[Any]
    Edge.from_nodes(
        source=factorization_node,
        target=write_node,
        source_key="factors",
        target_key="data",
    )

    # list[Any] -> list[int]
    with pytest.raises(TypeError):
        Edge.from_nodes(
            source=read_node,
            target=sum_node,
            source_key="data",
            target_key="values",
        )

    # list[Any] -> list[Any]
    Edge.from_nodes(
        source=read_node,
        target=write_node,
        source_key="data",
        target_key="data",
    )
