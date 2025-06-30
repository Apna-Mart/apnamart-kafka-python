"""Serialization utilities for Kafka messages."""

import json
from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from .exceptions import SerializationError


class Serializer(ABC):
    """Abstract base class for message serializers."""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        pass


class JSONSerializer(Serializer):
    """JSON serializer for message values."""

    def __init__(self, ensure_ascii: bool = False, **kwargs: Any) -> None:
        """Initialize JSON serializer.

        Args:
            ensure_ascii: If True, ensure all non-ASCII characters are escaped
            **kwargs: Additional arguments passed to json.dumps
        """
        self.ensure_ascii = ensure_ascii
        self.json_kwargs = kwargs

    def serialize(self, data: Any) -> bytes:
        """Serialize data to JSON bytes."""
        try:
            json_str = json.dumps(
                data, ensure_ascii=self.ensure_ascii, **self.json_kwargs
            )
            return json_str.encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to serialize data to JSON: {e}") from e


class StringSerializer(Serializer):
    """String serializer for message values."""

    def __init__(self, encoding: str = "utf-8"):
        """Initialize string serializer.

        Args:
            encoding: Text encoding to use
        """
        self.encoding = encoding

    def serialize(self, data: Any) -> bytes:
        """Serialize data to string bytes."""
        try:
            if isinstance(data, bytes):
                return data
            elif isinstance(data, str):
                return data.encode(self.encoding)
            else:
                return str(data).encode(self.encoding)
        except (UnicodeEncodeError, AttributeError) as e:
            raise SerializationError(f"Failed to serialize data to string: {e}") from e


class BytesSerializer(Serializer):
    """Bytes serializer for message values."""

    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode("utf-8")
        else:
            raise SerializationError(f"Cannot serialize {type(data)} to bytes")


class SerializerRegistry:
    """Registry for managing serializers."""

    def __init__(self) -> None:
        """Initialize with default serializers."""
        self._serializers = {
            "json": JSONSerializer(),
            "string": StringSerializer(),
            "bytes": BytesSerializer(),
        }

    def register(self, name: str, serializer: Serializer) -> None:
        """Register a custom serializer.

        Args:
            name: Name to register the serializer under
            serializer: Serializer instance
        """
        self._serializers[name] = serializer

    def get(self, name: str) -> Serializer:
        """Get a serializer by name.

        Args:
            name: Name of the serializer

        Returns:
            Serializer instance

        Raises:
            SerializationError: If serializer not found
        """
        if name not in self._serializers:
            raise SerializationError(f"Unknown serializer: {name}")
        return self._serializers[name]

    def list_serializers(self) -> list[str]:
        """List all available serializer names."""
        return list(self._serializers.keys())


# Global serializer registry
serializer_registry: SerializerRegistry = SerializerRegistry()


def serialize_key(key: Any) -> Optional[bytes]:
    """Serialize a message key.

    Args:
        key: Key to serialize

    Returns:
        Serialized key bytes or None
    """
    if key is None:
        return None

    if isinstance(key, bytes):
        return key
    elif isinstance(key, str):
        return key.encode("utf-8")
    else:
        return str(key).encode("utf-8")


def serialize_value(value: Any, serializer: Union[str, Serializer] = "json") -> bytes:
    """Serialize a message value.

    Args:
        value: Value to serialize
        serializer: Serializer to use (name or instance)

    Returns:
        Serialized value bytes
    """
    if isinstance(serializer, str):
        serializer = serializer_registry.get(serializer)

    return serializer.serialize(value)
