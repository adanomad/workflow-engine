# tests/test_node.py
from typing import Mapping
import pytest

from pydantic import ValidationError

from workflow_engine.core.data import build_data_type, get_data_fields
from workflow_engine.core.value import ValueType
from workflow_engine import BooleanValue, Data, StringValue, IntegerValue


@pytest.fixture
def ExampleData() -> type[Data]:
    """Test data class."""

    class ExampleData(Data):
        name: StringValue
        age: IntegerValue
        active: BooleanValue = None  # type: ignore

    return ExampleData


@pytest.fixture
def example_fields() -> Mapping[str, tuple[ValueType, bool]]:
    """Test fields."""

    return {
        "name": (StringValue, True),
        "age": (IntegerValue, True),
        "active": (BooleanValue, False),
    }


@pytest.mark.unit
def test_get_data_fields(
    ExampleData: type[Data], example_fields: Mapping[str, tuple[ValueType, bool]]
):
    """Test that get_data_fields returns the correct fields."""

    assert get_data_fields(ExampleData) == example_fields


@pytest.mark.unit
def test_build_data_type(
    ExampleData: type[Data], example_fields: Mapping[str, tuple[ValueType, bool]]
):
    """Test that build_data_type returns the correct class."""

    cls = build_data_type("ExampleData", example_fields)

    # can't exactly just test equality, instead we need to test that both
    # classes behave identically in instantiation, serialization, etc.
    assert cls.__name__ == ExampleData.__name__

    # Test that the class can be instantiated
    example_data = {
        "name": "John",
        "age": 30,
        "active": True,
    }

    # Test that each class behaves the same way when instantiating the data
    for kls in (cls, ExampleData):
        instance = kls(name="John", age=30, active=True)  # type: ignore
        deserialized = kls.model_validate(example_data)

        assert instance == deserialized
        assert instance.model_dump() == deserialized.model_dump() == example_data

        # missing optional field
        kls(name="John", age=30)  # type: ignore

        # missing required field
        with pytest.raises(ValidationError):
            kls(name="John", active=True)  # type: ignore

        # bad type
        with pytest.raises(ValidationError):
            kls(name="John", age="thirty", active=True)  # type: ignore

        # extra field
        with pytest.raises(ValidationError):
            kls(name="John", age=30, active=True, extra=1)  # type: ignore
