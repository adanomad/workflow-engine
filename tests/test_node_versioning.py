# tests/test_node_versioning.py
import warnings

import pytest
from pydantic import ValidationError

from workflow_engine import StringValue
from workflow_engine.nodes import ConstantStringNode
from workflow_engine.nodes.constant import ConstantString
from workflow_engine.utils.semver import LATEST_SEMANTIC_VERSION


class TestNodeVersioning:
    """Test node versioning functionality using ConstantStringNode."""

    @pytest.mark.unit
    def test_default_version_when_none_provided(self):
        """Test that when no version is provided, the node defaults to the current version."""
        # Create a node without specifying version
        node = ConstantStringNode(
            id="test_node",
            params=ConstantString(value=StringValue("test")),
        )

        # Should default to the current version from TYPE_INFO
        assert node.version == "0.4.0"
        assert node.version_tuple == (0, 4, 0)

    @pytest.mark.unit
    def test_serialization_includes_version(self):
        """Test that serializing the node includes the version."""
        # Create a node without specifying version
        node = ConstantStringNode(
            id="test_node",
            params=ConstantString(value=StringValue("test")),
        )

        # Serialize the node
        serialized = node.model_dump()

        # Should include the version
        assert "version" in serialized
        assert serialized["version"] == "0.4.0"

        # Should also work with JSON serialization
        json_str = node.model_dump_json()
        assert '"version":"0.4.0"' in json_str

    @pytest.mark.unit
    def test_older_version_triggers_warning(self):
        """Test that providing an older version triggers a warning."""
        # Create a node with an older version
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            node = ConstantStringNode(
                id="test_node",
                version="0.3.14",  # Older than current 0.4.0
                params=ConstantString(value=StringValue("test")),
            )

            # Should have triggered a warning
            assert len(w) == 1
            warning = w[0]
            assert issubclass(warning.category, UserWarning)

            warning_message = str(warning.message)
            assert (
                "Node version 0.3.14 is older than the latest version (0.4.0) supported by this workflow engine instance, and may need to be migrated."
                in warning_message
            )

            # Node should still be created successfully
            assert node.version == "0.3.14"
            assert node.version_tuple == (0, 3, 14)

    @pytest.mark.unit
    def test_newer_version_throws_error(self):
        """Test that providing a newer version throws an error."""
        with pytest.raises(ValidationError) as exc_info:
            ConstantStringNode(
                id="test_node",
                version="0.5.0",  # Newer than current 0.4.0
                params=ConstantString(value=StringValue("test")),
            )

        # Check that the error message is correct
        error_message = str(exc_info.value)
        assert (
            "Node version 0.5.0 is newer than the latest version (0.4.0) supported by this workflow engine instance."
            in error_message
        )

    @pytest.mark.unit
    def test_same_version_no_warning(self):
        """Test that providing the same version as current doesn't trigger warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            node = ConstantStringNode(
                id="test_node",
                version="0.4.0",  # Same as current
                params=ConstantString(value=StringValue("test")),
            )

            # Should not have triggered any warnings
            assert len(w) == 0
            assert node.version == "0.4.0"

    @pytest.mark.unit
    def test_latest_version_constant(self):
        """Test that using LATEST_SEMANTIC_VERSION constant works correctly."""
        node = ConstantStringNode(
            id="test_node",
            version=LATEST_SEMANTIC_VERSION,
            params=ConstantString(value=StringValue("test")),
        )

        # Should default to the current version
        assert node.version == "0.4.0"
        assert node.version_tuple == (0, 4, 0)

    @pytest.mark.unit
    def test_version_validation_during_deserialization(self):
        """Test that version validation works during deserialization."""
        # Test with older version - should work but warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            node_data = {
                "id": "test_node",
                "type": "ConstantString",
                "version": "0.3.0",
                "params": {"value": "test"},
            }

            node = ConstantStringNode.model_validate(node_data)
            assert len(w) == 1  # Should have warned
            assert node.version == "0.3.0"

        # Test with newer version - should fail
        with pytest.raises(ValidationError):
            node_data = {
                "id": "test_node",
                "type": "ConstantString",
                "version": "0.5.0",
                "params": {"value": "test"},
            }
            ConstantStringNode.model_validate(node_data)
