"""Tests for serialization utilities."""

import json

import pytest

from apnamart_kafka.exceptions import SerializationError
from apnamart_kafka.serializers import (
    BytesSerializer,
    JSONSerializer,
    SerializerRegistry,
    StringSerializer,
    serialize_key,
    serialize_value,
)


class TestJSONSerializer:
    """Test cases for JSONSerializer."""

    def test_serialize_dict(self):
        """Test serializing a dictionary."""
        serializer = JSONSerializer()
        data = {"key": "value", "number": 42}
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert json.loads(result.decode("utf-8")) == data

    def test_serialize_list(self):
        """Test serializing a list."""
        serializer = JSONSerializer()
        data = [1, 2, 3, "test"]
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert json.loads(result.decode("utf-8")) == data

    def test_serialize_string(self):
        """Test serializing a string."""
        serializer = JSONSerializer()
        data = "test string"
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert json.loads(result.decode("utf-8")) == data

    def test_serialize_number(self):
        """Test serializing a number."""
        serializer = JSONSerializer()
        data = 42
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert json.loads(result.decode("utf-8")) == data

    def test_serialize_invalid_data(self):
        """Test serializing invalid data."""
        serializer = JSONSerializer()
        # Object with circular reference
        obj = {}
        obj["self"] = obj

        with pytest.raises(SerializationError):
            serializer.serialize(obj)


class TestStringSerializer:
    """Test cases for StringSerializer."""

    def test_serialize_string(self):
        """Test serializing a string."""
        serializer = StringSerializer()
        data = "test string"
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert result == b"test string"

    def test_serialize_bytes(self):
        """Test serializing bytes."""
        serializer = StringSerializer()
        data = b"test bytes"
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert result == data

    def test_serialize_number(self):
        """Test serializing a number."""
        serializer = StringSerializer()
        data = 42
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert result == b"42"

    def test_serialize_with_encoding(self):
        """Test serializing with custom encoding."""
        serializer = StringSerializer(encoding="latin-1")
        data = "test"
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert result.decode("latin-1") == data


class TestBytesSerializer:
    """Test cases for BytesSerializer."""

    def test_serialize_bytes(self):
        """Test serializing bytes."""
        serializer = BytesSerializer()
        data = b"test bytes"
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert result == data

    def test_serialize_string(self):
        """Test serializing a string."""
        serializer = BytesSerializer()
        data = "test string"
        result = serializer.serialize(data)

        assert isinstance(result, bytes)
        assert result == b"test string"

    def test_serialize_invalid_data(self):
        """Test serializing invalid data."""
        serializer = BytesSerializer()
        data = {"key": "value"}

        with pytest.raises(SerializationError):
            serializer.serialize(data)


class TestSerializerRegistry:
    """Test cases for SerializerRegistry."""

    def test_default_serializers(self):
        """Test that default serializers are registered."""
        registry = SerializerRegistry()

        assert "json" in registry.list_serializers()
        assert "string" in registry.list_serializers()
        assert "bytes" in registry.list_serializers()

    def test_get_serializer(self):
        """Test getting a serializer."""
        registry = SerializerRegistry()

        json_serializer = registry.get("json")
        assert isinstance(json_serializer, JSONSerializer)

    def test_get_unknown_serializer(self):
        """Test getting an unknown serializer."""
        registry = SerializerRegistry()

        with pytest.raises(SerializationError, match="Unknown serializer"):
            registry.get("unknown")

    def test_register_custom_serializer(self):
        """Test registering a custom serializer."""
        registry = SerializerRegistry()
        custom_serializer = StringSerializer()

        registry.register("custom", custom_serializer)

        assert "custom" in registry.list_serializers()
        assert registry.get("custom") is custom_serializer


class TestUtilityFunctions:
    """Test cases for utility functions."""

    def test_serialize_key_none(self):
        """Test serializing None key."""
        result = serialize_key(None)
        assert result is None

    def test_serialize_key_bytes(self):
        """Test serializing bytes key."""
        key = b"test key"
        result = serialize_key(key)
        assert result == key

    def test_serialize_key_string(self):
        """Test serializing string key."""
        key = "test key"
        result = serialize_key(key)
        assert result == b"test key"

    def test_serialize_key_number(self):
        """Test serializing number key."""
        key = 42
        result = serialize_key(key)
        assert result == b"42"

    def test_serialize_value_with_string_serializer(self):
        """Test serializing value with string serializer name."""
        value = "test value"
        result = serialize_value(value, "string")
        assert result == b"test value"

    def test_serialize_value_with_serializer_instance(self):
        """Test serializing value with serializer instance."""
        value = "test value"
        serializer = StringSerializer()
        result = serialize_value(value, serializer)
        assert result == b"test value"
