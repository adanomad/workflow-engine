# tests/test_schema.py

"""
Tests two sets of functionalities:
1. that .to_value_cls() is the inverse of .to_value_schema()
2. that we can manually write JSON Schemas that will turn into the correct Value
   classes when .to_value_cls() is called on them.
3. that the aliasing system (e.g. { "title": "StringValue" } -> StringValue)
   works for all non-generic Value classes.
"""

import pytest

from workflow_engine import (
    BooleanValue,
    Data,
    Empty,
    FileValue,
    FloatValue,
    IntegerValue,
    NullValue,
    SequenceValue,
    StringMapValue,
    StringValue,
    WorkflowValue,
)
from workflow_engine.core.values import validate_value_schema
from workflow_engine.files import (
    JSONFileValue,
    JSONLinesFileValue,
    PDFFileValue,
    TextFileValue,
)


@pytest.mark.unit
def test_boolean_schema_roundtrip():
    cls = BooleanValue
    schema = cls.to_value_schema()
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == BooleanValue
    assert reconstructed_cls.to_value_schema() == schema

    original_instance = cls(True)
    reconstructed_instance = reconstructed_cls.model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_boolean_schema_manual():
    json_schema = {
        "type": "boolean",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == BooleanValue

    original_instance = BooleanValue(False)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_boolean_schema_aliasing():
    json_schema = {
        "title": "BooleanValue",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == BooleanValue

    original_instance = BooleanValue(True)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_integer_schema_roundtrip():
    cls = IntegerValue
    schema = cls.to_value_schema()
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == IntegerValue
    assert reconstructed_cls.to_value_schema() == schema

    original_instance = cls(42)
    reconstructed_instance = reconstructed_cls.model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_integer_schema_manual():
    json_schema = {
        "type": "integer",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == IntegerValue

    original_instance = IntegerValue(-42)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_integer_schema_aliasing():
    json_schema = {
        "title": "IntegerValue",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == IntegerValue

    original_instance = IntegerValue(2048)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_float_schema_roundtrip():
    cls = FloatValue
    schema = cls.to_value_schema()
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == FloatValue
    assert reconstructed_cls.to_value_schema() == schema

    original_instance = cls(3.14159)
    reconstructed_instance = reconstructed_cls.model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_float_schema_manual():
    json_schema = {
        "type": "number",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == FloatValue

    original_instance = FloatValue(2.71828)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_float_schema_aliasing():
    json_schema = {
        "title": "FloatValue",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == FloatValue

    original_instance = FloatValue(1.41421)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_null_schema_roundtrip():
    cls = NullValue
    schema = cls.to_value_schema()
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == NullValue
    assert reconstructed_cls.to_value_schema() == schema

    original_instance = cls(None)
    reconstructed_instance = reconstructed_cls.model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_null_schema_manual():
    json_schema = {
        "title": "NullValue",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == NullValue

    original_instance = NullValue(None)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_null_schema_aliasing():
    json_schema = {
        "title": "NullValue",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == NullValue

    original_instance = NullValue(None)
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_string_schema_roundtrip():
    cls = StringValue
    schema = cls.to_value_schema()
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == StringValue
    assert reconstructed_cls.to_value_schema() == schema

    original_instance = cls("hello wengine")
    reconstructed_instance = reconstructed_cls.model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_string_schema_manual():
    json_schema = {
        "type": "string",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == StringValue

    original_instance = StringValue("hi wengine")
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_string_schema_aliasing():
    json_schema = {
        "title": "StringValue",
    }
    schema = validate_value_schema(json_schema)
    assert schema.to_value_cls() == StringValue

    original_instance = StringValue("hey wengine")
    reconstructed_instance = schema.to_value_cls().model_validate(
        original_instance.model_dump()
    )
    assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_sequence_schema_roundtrip():
    for T in (
        BooleanValue,
        FloatValue,
        IntegerValue,
        NullValue,
        StringValue,
    ):
        cls = SequenceValue[T]
        schema = cls.to_value_schema()
        assert schema.to_value_cls() == SequenceValue[T]


@pytest.mark.unit
def test_sequence_schema_manual():
    for type, T in (
        ("boolean", BooleanValue),
        ("number", FloatValue),
        ("integer", IntegerValue),
        ("null", NullValue),
        ("string", StringValue),
    ):
        json_schema = {
            "type": "array",
            "items": {"type": type},
        }
        schema = validate_value_schema(json_schema)
        assert schema.to_value_cls() == SequenceValue[T]


@pytest.mark.unit
def test_sequence_schema_aliasing():
    for T in (
        BooleanValue,
        FloatValue,
        IntegerValue,
        NullValue,
        StringValue,
    ):
        json_schema = {
            "type": "array",
            "items": {"title": T.__name__},
        }
        schema = validate_value_schema(json_schema)
        assert schema.to_value_cls() == SequenceValue[T]


@pytest.mark.unit
def test_string_map_schema_roundtrip():
    for T in (
        BooleanValue,
        FloatValue,
        IntegerValue,
        NullValue,
        StringValue,
    ):
        cls = StringMapValue[T]
        schema = cls.to_value_schema()
        assert schema.to_value_cls() == cls


@pytest.mark.unit
def test_string_map_schema_manual():
    for type, T in (
        ("boolean", BooleanValue),
        ("number", FloatValue),
        ("integer", IntegerValue),
        ("null", NullValue),
        ("string", StringValue),
    ):
        json_schema = {
            "type": "object",
            "additionalProperties": {"type": type},
        }
        schema = validate_value_schema(json_schema)
        assert schema.to_value_cls() == StringMapValue[T]


@pytest.mark.unit
def test_string_map_schema_aliasing():
    for T in (
        BooleanValue,
        FloatValue,
        IntegerValue,
        NullValue,
        StringValue,
    ):
        json_schema = {
            "type": "object",
            "additionalProperties": {"title": T.__name__},
        }
        schema = validate_value_schema(json_schema)
        assert schema.to_value_cls() == StringMapValue[T]


@pytest.mark.unit
def test_super_recursive_schema_roundtrip():
    for cls in (
        StringMapValue[SequenceValue[StringMapValue[StringValue]]],
        SequenceValue[StringMapValue[SequenceValue[NullValue]]],
        StringMapValue[StringMapValue[StringMapValue[IntegerValue]]],
        SequenceValue[SequenceValue[SequenceValue[BooleanValue]]],
    ):
        schema = cls.to_value_schema()
        assert schema.to_value_cls() == cls


@pytest.mark.unit
def test_super_recursive_schema_manual():
    json_schema = {
        "type": "object",
        "additionalProperties": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": {"title": "StringValue"},
            },
        },
    }
    schema = validate_value_schema(json_schema)
    assert (
        schema.to_value_cls()
        == StringMapValue[SequenceValue[StringMapValue[StringValue]]]
    )


@pytest.mark.unit
def test_empty_schema_roundtrip():
    cls = Empty
    schema = cls.to_value_schema()
    data_value_cls = schema.to_value_cls()

    # for Empty, to_value_cls returns a new class not equal to the original
    # but it can serialize and deserialize instances of the original class
    original_instance = cls()
    reconstructed_instance = data_value_cls.model_validate(
        original_instance.model_dump()
    ).root
    # equality check fails because they are technically different classes,
    # but they have the exact same fields
    assert reconstructed_instance.__dict__ == original_instance.__dict__


# defined outside of test_data_schema_roundtrip to get a proper class name
class NestedData(Data):
    foo: StringValue
    bar: IntegerValue


@pytest.mark.unit
def test_data_schema_roundtrip():
    cls = NestedData
    schema = cls.to_value_schema()
    data_value_cls = schema.to_value_cls()

    # it can serialize and deserialize instances of the original class
    original_instance = cls(
        foo=StringValue("foo"),
        bar=IntegerValue(1),
    )
    reconstructed_instance = data_value_cls.model_validate(
        original_instance.model_dump()
    ).root
    # equality check fails because they are technically different classes,
    # but they have the exact same fields
    assert reconstructed_instance.foo == original_instance.foo
    assert reconstructed_instance.bar == original_instance.bar
    assert reconstructed_instance.__dict__ == original_instance.__dict__


@pytest.mark.unit
def test_data_schema_manual():
    json_schema = {
        "title": "NestedData",
        "type": "object",
        "properties": {
            "foo": {"type": "string"},
            "bar": {"type": "integer"},
        },
        "required": ["foo", "bar"],
    }
    schema = validate_value_schema(json_schema)
    data_value_cls = schema.to_value_cls()

    # it can serialize and deserialize instances of the original class
    original_instance = NestedData(
        foo=StringValue("bar"),
        bar=IntegerValue(12),
    )
    reconstructed_instance = data_value_cls.model_validate(
        original_instance.model_dump()
    ).root
    # equality check fails because they are technically different classes,
    # but they have the exact same fields
    assert reconstructed_instance.foo == original_instance.foo
    assert reconstructed_instance.bar == original_instance.bar
    assert reconstructed_instance.__dict__ == original_instance.__dict__


@pytest.mark.unit
def test_data_schema_aliasing():
    json_schema = {
        "title": "NestedData",
        "type": "object",
        "properties": {
            "foo": {"title": "StringValue"},
            "bar": {"title": "IntegerValue"},
        },
        "required": ["foo", "bar"],
    }
    schema = validate_value_schema(json_schema)
    data_value_cls = schema.to_value_cls()

    # it can serialize and deserialize instances of the original class
    original_instance = NestedData(
        foo=StringValue("foobar"),
        bar=IntegerValue(123),
    )
    reconstructed_instance = data_value_cls.model_validate(
        original_instance.model_dump()
    ).root
    # equality check fails because they are technically different classes,
    # but they have the exact same fields
    assert reconstructed_instance.foo == original_instance.foo
    assert reconstructed_instance.bar == original_instance.bar
    assert reconstructed_instance.__dict__ == original_instance.__dict__


@pytest.mark.unit
def test_file_schema_roundtrip():
    for cls in (
        FileValue,
        JSONFileValue,
        JSONLinesFileValue,
        PDFFileValue,
        TextFileValue,
    ):
        schema = cls.to_value_schema()
        reconstructed_cls = schema.to_value_cls()
        assert reconstructed_cls == cls
        assert reconstructed_cls.to_value_schema() == schema

        original_instance = cls.from_path("foo", foo="bar", bar="baz")
        reconstructed_instance = reconstructed_cls.model_validate(
            original_instance.model_dump()
        )
        assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_file_schema_aliasing():
    for cls in (
        FileValue,
        JSONFileValue,
        JSONLinesFileValue,
        PDFFileValue,
        TextFileValue,
    ):
        json_schema = {"title": cls.__name__}
        schema = validate_value_schema(json_schema)
        reconstructed_cls = schema.to_value_cls()
        assert reconstructed_cls == cls

        original_instance = cls.from_path("bar", bar="baz", baz="foo")
        reconstructed_instance = reconstructed_cls.model_validate(
            original_instance.model_dump()
        )
        assert reconstructed_instance == original_instance


@pytest.mark.unit
def test_workflow_schema_roundtrip():
    cls = WorkflowValue
    schema = cls.to_value_schema()
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == cls
    assert reconstructed_cls.to_value_schema() == schema

    with open("examples/addition.json", "r") as f:
        workflow_json = f.read().strip()

    workflow = cls.model_validate_json(workflow_json)
    reconstructed_workflow = reconstructed_cls.model_validate_json(workflow_json)
    assert reconstructed_workflow == workflow


@pytest.mark.unit
def test_workflow_schema_aliasing():
    cls = WorkflowValue
    json_schema = {"title": cls.__name__}
    schema = validate_value_schema(json_schema)
    reconstructed_cls = schema.to_value_cls()
    assert reconstructed_cls == WorkflowValue

    with open("examples/addition.json", "r") as f:
        workflow_json = f.read().strip()

    workflow = WorkflowValue.model_validate_json(workflow_json)
    reconstructed_workflow = schema.to_value_cls().model_validate_json(workflow_json)
    assert reconstructed_workflow == workflow
