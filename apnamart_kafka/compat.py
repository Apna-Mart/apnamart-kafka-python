"""Compatibility layer to maintain kafka-python-like API while using confluent-kafka."""

import warnings
from typing import Any, Dict, List, Optional, Union

from confluent_kafka import TopicPartition as ConfluentTopicPartition

from .producer import KafkaProducer as ConfluentKafkaProducer
from .consumer import KafkaConsumer as ConfluentKafkaConsumer, ConsumerMessage
from .config import KafkaConfig
from .consumer_config import KafkaConsumerConfig


class OffsetAndMetadata:
    """Compatibility wrapper for kafka-python's OffsetAndMetadata."""
    
    def __init__(self, offset: int, metadata: Optional[str] = None, leader_epoch: Optional[int] = None):
        """Initialize OffsetAndMetadata for compatibility.
        
        Args:
            offset: The offset value
            metadata: Optional metadata (ignored in confluent-kafka)
            leader_epoch: Optional leader epoch (ignored in confluent-kafka)
        """
        self.offset = offset
        self.metadata = metadata
        self.leader_epoch = leader_epoch


class TopicPartition:
    """Compatibility wrapper for kafka-python's TopicPartition."""
    
    def __init__(self, topic: str, partition: int, offset: int = None):
        """Initialize TopicPartition for compatibility.
        
        Args:
            topic: Topic name
            partition: Partition number
            offset: Optional offset
        """
        self.topic = topic
        self.partition = partition
        self.offset = offset
    
    def to_confluent(self) -> ConfluentTopicPartition:
        """Convert to confluent-kafka TopicPartition."""
        if self.offset is not None:
            return ConfluentTopicPartition(self.topic, self.partition, self.offset)
        return ConfluentTopicPartition(self.topic, self.partition)


class KafkaProducer:
    """Compatibility wrapper providing kafka-python-like Producer API."""
    
    def __init__(self, **configs):
        """Initialize producer with kafka-python style configuration.
        
        Args:
            **configs: kafka-python style configuration parameters
        """
        warnings.warn(
            "Using compatibility KafkaProducer. Consider migrating to apnamart_kafka.KafkaProducer "
            "for better performance.", 
            DeprecationWarning, 
            stacklevel=2
        )
        
        # Convert kafka-python config to our format
        apnamart_config = self._convert_producer_config(configs)
        self._producer = ConfluentKafkaProducer(config=apnamart_config)
    
    def _convert_producer_config(self, configs: Dict[str, Any]) -> KafkaConfig:
        """Convert kafka-python config to KafkaConfig."""
        # Map common kafka-python configs to our format
        bootstrap_servers = configs.get('bootstrap_servers', 'localhost:9092')
        if isinstance(bootstrap_servers, list):
            bootstrap_servers = ','.join(bootstrap_servers)
        
        return KafkaConfig(
            bootstrap_servers=bootstrap_servers,
            acks=str(configs.get('acks', 'all')),
            retries=configs.get('retries', 2147483647),
            retry_backoff_ms=configs.get('retry_backoff_ms', 100),
            request_timeout_ms=configs.get('request_timeout_ms', 30000),
            max_block_ms=configs.get('max_block_ms', 60000),
            max_request_size=configs.get('max_request_size', 1048576),
            batch_size=configs.get('batch_size', 16384),
            linger_ms=configs.get('linger_ms', 0),
            buffer_memory=configs.get('buffer_memory', 33554432),
            compression_type=configs.get('compression_type'),
            enable_idempotence=configs.get('enable_idempotence', True),
            max_in_flight_requests_per_connection=configs.get('max_in_flight_requests_per_connection', 5),
        )
    
    def send(
        self, 
        topic: str, 
        value: Any = None, 
        key: Any = None, 
        headers: Optional[Dict[str, bytes]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None
    ):
        """Send a message (kafka-python compatible interface).
        
        Returns:
            Future-like object for compatibility (though confluent-kafka doesn't use futures)
        """
        self._producer.send(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp_ms=timestamp_ms
        )
        
        # Return a mock future for compatibility
        class MockFuture:
            def get(self, timeout=None):
                return None  # In real implementation, this would track delivery
                
            def add_callback(self, callback):
                pass  # Mock callback support
                
            def add_errback(self, errback):
                pass  # Mock error callback support
        
        return MockFuture()
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages."""
        self._producer.flush(timeout)
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer."""
        self._producer.close()
    
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class KafkaConsumer:
    """Compatibility wrapper providing kafka-python-like Consumer API."""
    
    def __init__(self, *topics, **configs):
        """Initialize consumer with kafka-python style configuration.
        
        Args:
            *topics: Topics to subscribe to
            **configs: kafka-python style configuration parameters
        """
        warnings.warn(
            "Using compatibility KafkaConsumer. Consider migrating to apnamart_kafka.KafkaConsumer "
            "for better performance.", 
            DeprecationWarning, 
            stacklevel=2
        )
        
        # Convert kafka-python config to our format
        apnamart_config = self._convert_consumer_config(configs)
        self._consumer = ConfluentKafkaConsumer(config=apnamart_config)
        
        # Subscribe to topics if provided
        if topics:
            self._consumer.subscribe(list(topics))
    
    def _convert_consumer_config(self, configs: Dict[str, Any]) -> KafkaConsumerConfig:
        """Convert kafka-python config to KafkaConsumerConfig."""
        bootstrap_servers = configs.get('bootstrap_servers', 'localhost:9092')
        if isinstance(bootstrap_servers, list):
            bootstrap_servers = ','.join(bootstrap_servers)
        
        return KafkaConsumerConfig(
            bootstrap_servers=bootstrap_servers,
            group_id=configs.get('group_id', 'default-group'),
            auto_offset_reset=configs.get('auto_offset_reset', 'latest'),
            enable_auto_commit=configs.get('enable_auto_commit', True),
            auto_commit_interval_ms=configs.get('auto_commit_interval_ms', 5000),
            max_poll_interval_ms=configs.get('max_poll_interval_ms', 300000),
            session_timeout_ms=configs.get('session_timeout_ms', 30000),
            heartbeat_interval_ms=configs.get('heartbeat_interval_ms', 3000),
            fetch_min_bytes=configs.get('fetch_min_bytes', 1),
            fetch_max_bytes=configs.get('fetch_max_bytes', 52428800),
            fetch_max_wait_ms=configs.get('fetch_max_wait_ms', 500),
            max_partition_fetch_bytes=configs.get('max_partition_fetch_bytes', 1048576),
            consumer_timeout_ms=configs.get('consumer_timeout_ms', 1000),
            check_crcs=configs.get('check_crcs', True),
            exclude_internal_topics=configs.get('exclude_internal_topics', True),
        )
    
    def subscribe(self, topics: Union[str, List[str]], pattern: Optional[str] = None):
        """Subscribe to topics."""
        self._consumer.subscribe(topics, pattern)
    
    def assign(self, partitions: List[TopicPartition]):
        """Assign specific partitions."""
        confluent_partitions = [tp.to_confluent() for tp in partitions]
        self._consumer.assign(confluent_partitions)
    
    def poll(self, timeout_ms: int = 1000, max_records: Optional[int] = None):
        """Poll for messages (kafka-python compatible interface).
        
        Returns:
            Dict mapping TopicPartition to list of ConsumerRecord-like objects
        """
        messages = self._consumer.poll(timeout_ms, max_records)
        
        # Convert to kafka-python format: {TopicPartition: [ConsumerRecord]}
        result = {}
        for msg in messages:
            tp = TopicPartition(msg.topic, msg.partition)
            if tp not in result:
                result[tp] = []
            result[tp].append(msg)
        
        return result
    
    def commit(self, offsets: Optional[Dict[TopicPartition, OffsetAndMetadata]] = None):
        """Commit offsets."""
        if offsets:
            # Convert to confluent-kafka format
            confluent_offsets = {}
            for tp, offset_meta in offsets.items():
                confluent_tp = tp.to_confluent()
                confluent_offsets[confluent_tp] = offset_meta.offset
            self._consumer.commit_offsets(confluent_offsets)
        else:
            self._consumer.commit_offsets()
    
    def seek(self, partition: TopicPartition, offset: int):
        """Seek to specific offset."""
        confluent_tp = partition.to_confluent()
        self._consumer.seek(confluent_tp, offset)
    
    def seek_to_beginning(self, *partitions):
        """Seek to beginning of partitions."""
        if partitions:
            confluent_partitions = [tp.to_confluent() for tp in partitions]
            self._consumer.seek_to_beginning(confluent_partitions)
        else:
            self._consumer.seek_to_beginning()
    
    def seek_to_end(self, *partitions):
        """Seek to end of partitions."""
        if partitions:
            confluent_partitions = [tp.to_confluent() for tp in partitions]
            self._consumer.seek_to_end(confluent_partitions)
        else:
            self._consumer.seek_to_end()
    
    def close(self):
        """Close the consumer."""
        self._consumer.close()
    
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __iter__(self):
        """Iterate over messages."""
        return self
    
    def __next__(self):
        """Get next message."""
        while True:
            messages = self._consumer.poll(timeout_ms=1000)
            if messages:
                return messages[0]  # Return first message


# Expose compatibility classes
__all__ = [
    'KafkaProducer',
    'KafkaConsumer', 
    'TopicPartition',
    'OffsetAndMetadata',
]