"""Unit tests for serialization functionality."""

import json

from apnamart_kafka.client import deserialize, serialize


class TestSerialization:
    """Test serialization and deserialization functions."""

    def test_serialize_string(self):
        """Test string serialization."""
        result = serialize("test string")
        assert result == b"test string"

    def test_serialize_bytes(self):
        """Test bytes serialization (pass-through)."""
        test_bytes = b"test bytes"
        result = serialize(test_bytes)
        assert result == test_bytes

    def test_serialize_dict(self):
        """Test dictionary serialization to JSON."""
        test_dict = {"key": "value", "number": 42}
        result = serialize(test_dict)
        expected = json.dumps(test_dict).encode("utf-8")
        assert result == expected

    def test_serialize_list(self):
        """Test list serialization to JSON."""
        test_list = [1, 2, 3, "four"]
        result = serialize(test_list)
        expected = json.dumps(test_list).encode("utf-8")
        assert result == expected

    def test_serialize_number(self):
        """Test number serialization to JSON."""
        result = serialize(42)
        expected = json.dumps(42).encode("utf-8")
        assert result == expected

    def test_serialize_boolean(self):
        """Test boolean serialization to JSON."""
        result = serialize(True)
        expected = json.dumps(True).encode("utf-8")
        assert result == expected

    def test_serialize_none(self):
        """Test None serialization."""
        result = serialize(None)
        expected = json.dumps(None).encode("utf-8")
        assert result == expected

    def test_deserialize_valid_json(self):
        """Test deserializing valid JSON bytes."""
        test_data = {"key": "value", "number": 42}
        json_bytes = json.dumps(test_data).encode("utf-8")
        result = deserialize(json_bytes)
        assert result == test_data

    def test_deserialize_invalid_json(self):
        """Test deserializing invalid JSON returns decoded string."""
        invalid_json = b"not valid json {"
        result = deserialize(invalid_json)
        assert result == "not valid json {"

    def test_deserialize_string_bytes(self):
        """Test deserializing non-JSON string bytes."""
        string_bytes = b"just a string"
        result = deserialize(string_bytes)
        assert result == "just a string"

    def test_deserialize_empty_bytes(self):
        """Test deserializing empty bytes."""
        result = deserialize(b"")
        assert result == ""

    def test_roundtrip_serialization(self):
        """Test round-trip serialization/deserialization."""
        original_data = {
            "string": "test",
            "number": 42,
            "boolean": True,
            "null": None,
            "array": [1, 2, 3],
            "nested": {"inner": "value"},
        }

        # Serialize then deserialize
        serialized = serialize(original_data)
        deserialized = deserialize(serialized)

        assert deserialized == original_data

    def test_serialize_complex_types(self):
        """Test serialization of complex nested types."""
        complex_data = {
            "level1": {"level2": {"level3": ["a", "b", {"deep": "value"}]}},
            "unicode": "Hello 世界 ",
            "numbers": [1, 2.5, -3, 0],
            "mixed_array": ["string", 42, True, None],
        }

        serialized = serialize(complex_data)
        deserialized = deserialize(serialized)

        assert deserialized == complex_data
