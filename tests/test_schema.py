# tests/test_schema.py
import pytest
from pydantic import ValidationError

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
from workflow_engine.core.value import (
    BooleanValue,
    FloatValue,
    IntegerValue,
    NullValue,
    SequenceValue,
    StringMapValue,
    StringValue,
)


@pytest.mark.unit
def test_integer_schema():
    """Test IntegerJSONSchema creation and validation."""
    # Basic integer schema
    schema = IntegerJSONSchema(type="integer")
    assert schema.value_type is IntegerValue

    # Integer with constraints
    schema = IntegerJSONSchema(type="integer", minimum=0, maximum=100)
    assert schema.value_type is IntegerValue
    assert schema.minimum == 0
    assert schema.maximum == 100

    # Invalid range
    with pytest.raises(ValidationError):
        IntegerJSONSchema(type="integer", minimum=100, maximum=0)


@pytest.mark.unit
def test_number_schema():
    """Test NumberJSONSchema creation and validation."""
    # Basic number schema
    schema = NumberJSONSchema(type="number")
    assert schema.value_type is FloatValue

    # Number with constraints
    schema = NumberJSONSchema(type="number", minimum=0.0, maximum=100.0)
    assert schema.value_type is FloatValue
    assert schema.minimum == 0.0
    assert schema.maximum == 100.0

    # Invalid range
    with pytest.raises(ValidationError):
        NumberJSONSchema(type="number", minimum=100.0, maximum=0.0)


@pytest.mark.unit
def test_boolean_schema():
    """Test BooleanJSONSchema creation and validation."""
    schema = BooleanJSONSchema(type="boolean")
    assert schema.value_type is BooleanValue


@pytest.mark.unit
def test_null_schema():
    """Test NullJSONSchema creation and validation."""
    schema = NullJSONSchema(type="null")
    assert schema.value_type is NullValue


@pytest.mark.unit
def test_string_schema():
    """Test StringJSONSchema creation and validation."""
    # Basic string schema
    schema = StringJSONSchema(type="string")
    assert schema.value_type is StringValue

    # String with constraints
    schema = StringJSONSchema(
        type="string", minLength=1, maxLength=100, pattern="^[a-z]+$"
    )
    assert schema.value_type is StringValue
    assert schema.minLength == 1
    assert schema.maxLength == 100
    assert schema.pattern == "^[a-z]+$"

    # Invalid range
    with pytest.raises(ValidationError):
        StringJSONSchema(type="string", minLength=100, maxLength=1)


@pytest.mark.unit
def test_sequence_schema():
    """Test SequenceJSONSchema creation and validation."""
    # Basic array schema
    items_schema = JSONSchema.model_validate({"type": "integer"})
    schema = SequenceJSONSchema(type="array", items=items_schema)
    assert schema.value_type == SequenceValue[IntegerValue]

    # Array with constraints
    schema = SequenceJSONSchema(
        type="array", items=items_schema, minItems=1, maxItems=10, uniqueItems=True
    )
    assert schema.value_type == SequenceValue[IntegerValue]
    assert schema.minItems == 1
    assert schema.maxItems == 10
    assert schema.uniqueItems is True


@pytest.mark.unit
def test_string_map_schema():
    """Test StringMapJSONSchema creation and validation."""
    # Basic object schema with additionalProperties
    value_schema = JSONSchema.model_validate({"type": "string"})
    schema = StringMapJSONSchema(type="object", additionalProperties=value_schema)
    assert schema.value_type == StringMapValue[StringValue]


@pytest.mark.unit
def test_object_schema():
    """Test ObjectJSONSchema creation and validation."""
    # Object with properties
    properties = {
        "name": JSONSchema.model_validate({"type": "string"}),
        "age": JSONSchema.model_validate({"type": "integer"}),
    }
    schema = ObjectJSONSchema(type="object", properties=properties, required={"name"})

    # The value_type should be a DataValue wrapping a Data type
    value_type = schema.value_type
    assert "DataValue" in str(value_type)

    # Test with no required fields
    schema = ObjectJSONSchema(type="object", properties=properties, required=set())
    assert schema.required == set()


@pytest.mark.unit
def test_json_schema_ref():
    """Test JSONSchemaRef creation and validation."""
    # Test with existing Value type
    schema = JSONSchemaRef.model_validate_json('{"$ref": "IntegerValue"}')
    assert schema.value_type is IntegerValue

    # Test with non-existent Value type
    schema = JSONSchemaRef.model_validate_json('{"$ref": "NonExistentValue"}')
    with pytest.raises(
        ValueError, match='Value type "NonExistentValue" is not registered'
    ):
        _ = schema.value_type


@pytest.mark.unit
def test_json_schema_root_model():
    """Test JSONSchema RootModel with different schema types."""
    # Test integer schema
    schema = JSONSchema.model_validate({"type": "integer"})
    assert schema.value_type is IntegerValue

    # Test string schema
    schema = JSONSchema.model_validate({"type": "string"})
    assert schema.value_type is StringValue

    # Test boolean schema
    schema = JSONSchema.model_validate({"type": "boolean"})
    assert schema.value_type is BooleanValue

    # Test null schema
    schema = JSONSchema.model_validate({"type": "null"})
    assert schema.value_type is NullValue

    # Test number schema
    schema = JSONSchema.model_validate({"type": "number"})
    assert schema.value_type is FloatValue


@pytest.mark.unit
def test_json_schema_with_ref():
    """Test JSONSchema with $ref fields."""
    # Test direct $ref
    schema = JSONSchema.model_validate_json('{"$ref": "IntegerValue"}')
    assert schema.value_type is IntegerValue

    # Test object with $ref properties
    schema = JSONSchema.model_validate_json("""{
        "type": "object",
        "properties": {"value": {"$ref": "IntegerValue"}},
        "required": ["value"]
    }""")
    # Should create an ObjectJSONSchema with JSONSchemaRef in properties
    assert isinstance(schema.root, ObjectJSONSchema)
    assert "value" in schema.root.properties


@pytest.mark.unit
def test_complex_nested_schemas():
    """Test complex nested schema structures."""
    # Array of objects
    object_schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
        "required": {"name"},
    }

    array_schema = {"type": "array", "items": object_schema}

    schema = JSONSchema.model_validate(array_schema)
    # Should create a SequenceJSONSchema with ObjectJSONSchema as items
    assert isinstance(schema.root, SequenceJSONSchema)
    assert isinstance(schema.root.items.root, ObjectJSONSchema)


@pytest.mark.unit
def test_schema_validation_errors():
    """Test schema validation error cases."""
    # Invalid type
    with pytest.raises(ValidationError):
        JSONSchema.model_validate({"type": "invalid_type"})

    # Missing required fields
    with pytest.raises(ValidationError):
        SequenceJSONSchema.model_validate({"type": "array"})  # missing items

    with pytest.raises(ValidationError):
        StringMapJSONSchema.model_validate(
            {"type": "object"}
        )  # missing additionalProperties

    # Invalid $ref
    with pytest.raises(ValidationError):
        JSONSchemaRef.model_validate({})  # missing $ref


@pytest.mark.unit
def test_schema_extra_fields():
    """Test that extra fields are allowed due to extra='allow' config."""
    # Add extra fields to integer schema
    schema = IntegerJSONSchema.model_validate(
        {
            "type": "integer",
            "minimum": 0,
            "maximum": 100,
            "description": "A test integer",
            "example": 42,
        }
    )
    assert schema.value_type is IntegerValue
    assert schema.minimum == 0
    assert schema.maximum == 100
    # Extra fields should be preserved
    assert schema.model_extra is not None
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
    original_schema = JSONSchema.model_validate_json("""{
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

    # Test that the schema was parsed correctly
    assert isinstance(original_schema.root, ObjectJSONSchema)
    assert "numbers" in original_schema.root.properties
    assert "config" in original_schema.root.properties

    # Test that we can access the value_type
    value_type = original_schema.value_type
    assert "DataValue" in str(value_type)


@pytest.mark.unit
def test_schema_from_pydantic_model():
    """Test creating schema from Pydantic model JSON schema."""
    from workflow_engine.core.data import build_data_type
    from workflow_engine.core.value import IntegerValue, StringValue

    # Create a simple Data type
    TestData = build_data_type(
        "TestData", {"name": (StringValue, True), "age": (IntegerValue, False)}
    )

    # Get the JSON schema
    json_schema = TestData.model_json_schema()

    # Parse it with our JSONSchema
    schema = JSONSchema.model_validate(json_schema)

    # Should create an ObjectJSONSchema
    assert isinstance(schema.root, ObjectJSONSchema)
    assert "name" in schema.root.properties
    assert "age" in schema.root.properties
    assert "name" in schema.root.required
    assert "age" not in schema.root.required
