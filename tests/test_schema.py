# tests/test_schema.py
import pytest
from pydantic import ValidationError

from workflow_engine import (
    BooleanValue,
    DataValue,
    FileValue,
    FloatValue,
    IntegerValue,
    NullValue,
    SequenceValue,
    StringMapValue,
    StringValue,
)
from workflow_engine.core.schema import (
    BooleanJSONSchema,
    IntegerJSONSchema,
    JSONSchema,
    JSONSchemaRef,
    NullJSONSchema,
    NumberJSONSchema,
    ObjectJSONSchema,
    SequenceJSONSchema,
    StringJSONSchema,
    StringMapJSONSchema,
)


@pytest.mark.unit
def test_integer_schema():
    """Test IntegerJSONSchema creation and validation."""
    # Basic integer schema
    schema = IntegerJSONSchema(type="integer")
    assert schema.value_type is IntegerValue

    # attempt to deserialize various data
    data = schema.value_type.model_validate(1)
    assert isinstance(data, IntegerValue)
    assert data.root == 1

    with pytest.raises(ValidationError):  # wrong type
        _ = schema.value_type.model_validate(3.14)


@pytest.mark.unit
def test_number_schema():
    """Test NumberJSONSchema creation and validation."""
    # Basic number schema
    schema = NumberJSONSchema(type="number")
    assert schema.value_type is FloatValue

    # attempt to deserialize various data
    data = schema.value_type.model_validate(1.0)
    assert isinstance(data, FloatValue)
    assert data.root == 1.0

    with pytest.raises(ValidationError):  # wrong type
        _ = schema.value_type.model_validate("not a number")


@pytest.mark.unit
def test_boolean_schema():
    """Test BooleanJSONSchema creation and validation."""
    schema = BooleanJSONSchema(type="boolean")
    assert schema.value_type is BooleanValue

    # attempt to deserialize various data
    data = schema.value_type.model_validate(True)
    assert isinstance(data, BooleanValue)
    assert data.root is True

    with pytest.raises(ValidationError):  # wrong type
        _ = schema.value_type.model_validate("not a boolean")


@pytest.mark.unit
def test_null_schema():
    """Test NullJSONSchema creation and validation."""
    schema = NullJSONSchema(type="null")
    assert schema.value_type is NullValue

    # attempt to deserialize various data
    data = schema.value_type.model_validate(None)
    assert isinstance(data, NullValue)
    assert data.root is None

    with pytest.raises(ValidationError):  # wrong type
        _ = schema.value_type.model_validate("not a null")


@pytest.mark.unit
def test_string_schema():
    """Test StringJSONSchema creation and validation."""
    # Basic string schema
    schema = StringJSONSchema(type="string")
    assert schema.value_type is StringValue

    # attempt to deserialize various data
    data = schema.value_type.model_validate("hello")
    assert isinstance(data, StringValue)
    assert data.root == "hello"

    with pytest.raises(ValidationError):  # wrong type
        _ = schema.value_type.model_validate(1)

    # Invalid range
    with pytest.raises(ValidationError):
        StringJSONSchema(type="string", minLength=100, maxLength=1)


@pytest.mark.unit
def test_sequence_schema():
    """Test SequenceJSONSchema creation and validation."""
    # Basic array schema
    schema = SequenceJSONSchema(
        type="array",
        items=IntegerJSONSchema(type="integer"),
    )
    value_type = schema.value_type
    assert value_type == SequenceValue[IntegerValue]

    # attempt to deserialize various data
    data = value_type.model_validate([1, 2, 3])
    assert isinstance(data, SequenceValue)
    assert data.root == [IntegerValue(1), IntegerValue(2), IntegerValue(3)]

    with pytest.raises(ValidationError):  # wrong type
        _ = value_type.model_validate([1, "not an integer", 3])


@pytest.mark.unit
def test_string_map_schema():
    """Test StringMapJSONSchema creation and validation."""
    # Basic object schema with additionalProperties
    schema = StringMapJSONSchema(
        type="object",
        additionalProperties=BooleanJSONSchema(type="boolean"),
    )
    value_type = schema.value_type
    assert value_type == StringMapValue[BooleanValue]

    # attempt to deserialize various data
    data = value_type.model_validate({"a": True, "b": False})
    assert isinstance(data, StringMapValue)
    assert data.root["a"] == BooleanValue(True)  # type: ignore
    assert data.root["b"] == BooleanValue(False)  # type: ignore

    with pytest.raises(ValidationError):  # wrong type for a
        _ = value_type.model_validate({"a": "not a boolean", "b": False})


@pytest.mark.unit
def test_object_schema():
    """Test ObjectJSONSchema creation and validation."""
    # Object with properties
    schema = ObjectJSONSchema(
        type="object",
        properties={
            "name": StringJSONSchema(type="string"),
            "age": IntegerJSONSchema(type="integer"),
        },
        required={"name"},
    )

    # The value_type should be a DataValue wrapping a Data type
    value_type = schema.value_type
    assert issubclass(value_type, DataValue)

    # attempt to deserialize various data
    data = value_type.model_validate({"name": "John"}).root
    assert data.name == StringValue("John")  # type: ignore

    with pytest.raises(ValidationError):  # missing name
        _ = value_type.model_validate({"age": 30})

    data = value_type.model_validate({"name": "John", "age": 30}).root
    assert data.name == StringValue("John")  # type: ignore
    assert data.age == IntegerValue(30)  # type: ignore


@pytest.mark.unit
def test_json_schema_ref():
    """Test JSONSchemaRef creation and validation."""
    good_schema = JSONSchemaRef.from_name("FileValue")
    assert good_schema.value_type is FileValue

    # Test with non-existent Value type
    bad_schema = JSONSchemaRef.from_name("NonExistentValue")
    with pytest.raises(
        ValueError, match='Value type "NonExistentValue" is not registered'
    ):
        _ = bad_schema.value_type

    # attempt to deserialize various data
    data = good_schema.value_type.model_validate(
        {"path": "test.txt", "metadata": {"size": 100}}
    )
    assert isinstance(data, FileValue)
    assert data.root.path == "test.txt"
    assert data.root.metadata["size"] == 100

    with pytest.raises(ValidationError):  # missing path
        _ = good_schema.value_type.model_validate({"metadata": {"size": 100}})


@pytest.mark.unit
def test_json_schema_root_model():
    """Test JSONSchema RootModel with different schema types."""
    # Test integer schema
    schema = JSONSchema.loads('{"type": "integer"}')
    assert schema.value_type is IntegerValue

    # Test string schema
    schema = JSONSchema.loads('{"type": "string"}')
    assert schema.value_type is StringValue

    # Test boolean schema
    schema = JSONSchema.loads('{"type": "boolean"}')
    assert schema.value_type is BooleanValue

    # Test null schema
    schema = JSONSchema.loads('{"type": "null"}')
    assert schema.value_type is NullValue

    # Test number schema
    schema = JSONSchema.loads('{"type": "number"}')
    assert schema.value_type is FloatValue


@pytest.mark.unit
def test_schema_validation_errors():
    """Test schema validation error cases."""
    # Invalid type
    with pytest.raises(ValidationError):
        JSONSchema.load({"type": "invalid_type"})

    # Missing required fields
    with pytest.raises(ValidationError):
        JSONSchema.load({"type": "array"})  # missing items

    with pytest.raises(ValidationError):
        JSONSchema.load({"type": "object"})  # missing additionalProperties

    # Invalid $ref
    with pytest.raises(ValidationError):
        JSONSchemaRef.model_validate({})  # missing $ref


@pytest.mark.unit
def test_schema_extra_fields():
    """Test that extra fields are allowed due to extra='allow' config."""
    # Add extra fields to integer schema
    schema = JSONSchema.load(
        {
            "type": "integer",
            "title": "Test Integer",
            "description": "A test integer",
            "example": 42,
        }
    )
    assert schema.value_type is IntegerValue
    assert schema.model_extra is not None
    assert schema.model_extra["title"] == "Test Integer"
    assert schema.model_extra["description"] == "A test integer"
    assert schema.model_extra["example"] == 42


@pytest.mark.unit
def test_schema_frozen_behavior():
    """Test that schemas are frozen (immutable)."""
    schema = IntegerJSONSchema(type="integer", minimum=0, maximum=100)

    # Should not be able to modify fields
    with pytest.raises(ValidationError):
        schema.minimum = 10


@pytest.mark.unit
def test_schema_serialization():
    """Test schema serialization and deserialization."""
    # Create a complex schema
    schema = JSONSchema.loads("""{
        "type": "object",
        "properties": {
            "numbers": {"type": "array", "items": {"type": "integer"}},
            "config": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            }
        },
        "required": ["numbers"]
    }""")

    assert schema == ObjectJSONSchema(
        type="object",
        properties={
            "numbers": SequenceJSONSchema(
                type="array",
                items=IntegerJSONSchema(type="integer"),
            ),
            "config": StringMapJSONSchema(
                type="object",
                additionalProperties=StringJSONSchema(type="string"),
            ),
        },
        required=frozenset({"numbers"}),
    )


@pytest.mark.unit
def test_schema_from_pydantic_model():
    """Test round-trip Data to JSONSchema to Data."""
    from workflow_engine.core.data import build_data_type
    from workflow_engine.core.value import IntegerValue, StringValue

    # Create a simple Data type
    TestData = build_data_type(
        "TestData",
        {
            "name": (StringValue, True),
            "age": (IntegerValue, False),
        },
    )

    # Get the JSON schema
    json_schema_dict = TestData.model_json_schema()

    # Parse it with our JSONSchema
    schema = JSONSchema.load(json_schema_dict)

    # Should create an ObjectJSONSchema
    assert isinstance(schema, ObjectJSONSchema)
    assert issubclass(schema.value_type, DataValue)
