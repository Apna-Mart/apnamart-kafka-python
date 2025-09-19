#!/usr/bin/env python3
"""
Advanced Microservices Communication Patterns

This example demonstrates how to use Kafka for microservices communication,
including event sourcing, CQRS, and saga patterns.
"""

import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from apnamart_kafka import Config, Consumer, Producer, TransactionalProducer


@dataclass
class Event:
    """Domain event structure."""

    event_id: str
    event_type: str
    aggregate_id: str
    data: Dict[str, Any]
    timestamp: float
    version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": self.aggregate_id,
            "data": self.data,
            "timestamp": self.timestamp,
            "version": self.version,
        }


class EventStore:
    """Simple event store implementation using Kafka."""

    def __init__(self, producer: Producer, topic: str = "event-store"):
        self.producer = producer
        self.topic = topic

    def append_event(self, event: Event) -> None:
        """Append event to the event store."""
        self.producer.send(self.topic, event.to_dict(), key=event.aggregate_id)

    def append_events(self, events: list[Event]) -> None:
        """Append multiple events atomically."""
        messages = [
            (self.topic, event.to_dict(), event.aggregate_id) for event in events
        ]
        self.producer.send_batch(messages)


class EventSourcedOrderService:
    """Order service using event sourcing pattern."""

    def __init__(self, event_store: EventStore):
        self.event_store = event_store

    def create_order(self, customer_id: str, items: list[Dict[str, Any]]) -> str:
        """Create a new order."""
        order_id = str(uuid.uuid4())

        # Create order creation event
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type="OrderCreated",
            aggregate_id=order_id,
            data={
                "customer_id": customer_id,
                "items": items,
                "status": "pending",
                "total": sum(item["price"] * item["quantity"] for item in items),
            },
            timestamp=time.time(),
        )

        self.event_store.append_event(event)
        print(f" Order {order_id} created for customer {customer_id}")
        return order_id

    def process_payment(self, order_id: str, payment_method: str) -> None:
        """Process payment for an order."""
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type="PaymentProcessed",
            aggregate_id=order_id,
            data={
                "payment_method": payment_method,
                "status": "paid",
                "processed_at": time.time(),
            },
            timestamp=time.time(),
        )

        self.event_store.append_event(event)
        print(f" Payment processed for order {order_id}")

    def ship_order(self, order_id: str, tracking_number: str) -> None:
        """Ship an order."""
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type="OrderShipped",
            aggregate_id=order_id,
            data={
                "tracking_number": tracking_number,
                "status": "shipped",
                "shipped_at": time.time(),
            },
            timestamp=time.time(),
        )

        self.event_store.append_event(event)
        print(f" Order {order_id} shipped with tracking {tracking_number}")


class SagaOrchestrator:
    """Saga pattern for distributed transactions."""

    def __init__(self, producer: Producer, saga_topic: str = "saga-events"):
        self.producer = producer
        self.saga_topic = saga_topic

    def start_order_saga(self, order_data: Dict[str, Any]) -> str:
        """Start an order processing saga."""
        saga_id = str(uuid.uuid4())

        saga_event = {
            "saga_id": saga_id,
            "saga_type": "OrderProcessing",
            "step": "started",
            "order_data": order_data,
            "timestamp": time.time(),
        }

        self.producer.send(self.saga_topic, saga_event, key=saga_id)
        print(f" Started order saga {saga_id}")
        return saga_id

    def handle_saga_step(
        self, saga_id: str, step: str, success: bool, data: Dict[str, Any]
    ) -> None:
        """Handle a saga step completion."""
        saga_event = {
            "saga_id": saga_id,
            "step": step,
            "success": success,
            "data": data,
            "timestamp": time.time(),
        }

        if success:
            saga_event["next_step"] = self._get_next_step(step)
        else:
            saga_event["compensation_needed"] = True
            saga_event["failed_step"] = step

        self.producer.send(self.saga_topic, saga_event, key=saga_id)
        print(f" Saga {saga_id} step '{step}': {'SUCCESS' if success else 'FAILED'}")

    def _get_next_step(self, current_step: str) -> Optional[str]:
        """Get the next step in the saga."""
        steps = [
            "inventory_check",
            "payment_process",
            "shipment_create",
            "notification_send",
        ]
        try:
            current_index = steps.index(current_step)
            return steps[current_index + 1] if current_index < len(steps) - 1 else None
        except ValueError:
            return steps[0] if current_step == "started" else None


class CQRSReadModelUpdater:
    """CQRS read model updater that listens to events."""

    def __init__(self, consumer: Consumer):
        self.consumer = consumer
        self.read_models: Dict[str, Dict[str, Any]] = {}

    def start_processing(self) -> None:
        """Start processing events to update read models."""
        print(" CQRS Read Model Updater started")

        for message in self.consumer:
            try:
                event_data = message.value
                self._update_read_model(event_data)
                self.consumer.commit(message)
            except Exception as e:
                print(f" Error processing event: {e}")

    def _update_read_model(self, event: Dict[str, Any]) -> None:
        """Update read model based on event."""
        event_type = event.get("event_type")
        aggregate_id = event.get("aggregate_id")

        if event_type == "OrderCreated":
            self.read_models[aggregate_id] = {
                "order_id": aggregate_id,
                "customer_id": event["data"]["customer_id"],
                "status": event["data"]["status"],
                "total": event["data"]["total"],
                "items": event["data"]["items"],
                "created_at": event["timestamp"],
            }
            print(f" Created read model for order {aggregate_id}")

        elif event_type == "PaymentProcessed":
            if aggregate_id in self.read_models:
                self.read_models[aggregate_id]["status"] = "paid"
                self.read_models[aggregate_id]["payment_method"] = event["data"][
                    "payment_method"
                ]
                print(f" Updated read model: order {aggregate_id} paid")

        elif event_type == "OrderShipped":
            if aggregate_id in self.read_models:
                self.read_models[aggregate_id]["status"] = "shipped"
                self.read_models[aggregate_id]["tracking_number"] = event["data"][
                    "tracking_number"
                ]
                print(f" Updated read model: order {aggregate_id} shipped")

    def get_order_view(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order view from read model."""
        return self.read_models.get(order_id)


def microservices_communication_example():
    """Demonstrate microservices communication patterns."""
    print("️  Microservices Communication Example")
    print("-" * 40)

    config = Config()

    # Event sourcing example
    with Producer(config) as producer:
        event_store = EventStore(producer)
        order_service = EventSourcedOrderService(event_store)

        # Create and process an order
        order_id = order_service.create_order(
            customer_id="customer-123",
            items=[
                {"product": "laptop", "price": 1299.99, "quantity": 1},
                {"product": "mouse", "price": 29.99, "quantity": 2},
            ],
        )

        order_service.process_payment(order_id, "credit_card")
        order_service.ship_order(order_id, "TRACK123456")


def saga_pattern_example():
    """Demonstrate saga pattern for distributed transactions."""
    print("\n Saga Pattern Example")
    print("-" * 30)

    config = Config()

    with Producer(config) as producer:
        saga = SagaOrchestrator(producer)

        order_data = {
            "customer_id": "customer-456",
            "items": [{"product": "tablet", "price": 599.99, "quantity": 1}],
            "total": 599.99,
        }

        saga_id = saga.start_order_saga(order_data)

        # Simulate saga steps
        saga.handle_saga_step(
            saga_id, "inventory_check", True, {"items_available": True}
        )
        saga.handle_saga_step(
            saga_id, "payment_process", True, {"transaction_id": "TXN789"}
        )
        saga.handle_saga_step(
            saga_id, "shipment_create", False, {"error": "No available drivers"}
        )


def cqrs_pattern_example():
    """Demonstrate CQRS pattern with separate read/write models."""
    print("\n CQRS Pattern Example")
    print("-" * 25)

    # Simulate CQRS read model updater
    print(" CQRS read model would process events here")
    print("   (In real implementation, this would run as a separate service)")

    # Configuration for read model updater
    config = Config(group_id="cqrs-read-model-updater", auto_offset_reset="earliest")
    print(f" Read model updater would use config: {config.group_id}")

    # Example of how the read model updater would work:
    sample_events = [
        {
            "event_type": "OrderCreated",
            "aggregate_id": "order-789",
            "data": {
                "customer_id": "customer-789",
                "status": "pending",
                "total": 299.99,
                "items": [{"product": "book", "price": 299.99, "quantity": 1}],
            },
            "timestamp": time.time(),
        }
    ]

    # Simulate processing
    for event in sample_events:
        print(f" Processing {event['event_type']} for {event['aggregate_id']}")


def transactional_outbox_pattern():
    """Demonstrate transactional outbox pattern."""
    print("\n Transactional Outbox Pattern")
    print("-" * 35)

    config = Config()
    tx_id = f"outbox-tx-{uuid.uuid4().hex[:8]}"

    with TransactionalProducer(tx_id, config) as tx_producer:
        try:
            tx_producer.begin()

            # Simulate database transaction + outbox events
            outbox_events = [
                {
                    "topic": "user-events",
                    "value": {
                        "event_type": "UserRegistered",
                        "user_id": "user-123",
                        "email": "user@example.com",
                        "timestamp": time.time(),
                    },
                },
                {
                    "topic": "notification-events",
                    "value": {
                        "event_type": "WelcomeEmailRequested",
                        "user_id": "user-123",
                        "template": "welcome",
                        "timestamp": time.time(),
                    },
                },
            ]

            # Send all outbox events transactionally
            results = tx_producer.send_batch_transactional(outbox_events)

            # Commit transaction (simulating DB commit)
            tx_producer.commit()

            successful = sum(1 for r in results if r["success"])
            print(
                f" Transactional outbox: {successful}/{len(outbox_events)} events published"
            )

        except Exception as e:
            tx_producer.abort()
            print(f" Transaction aborted: {e}")


def main():
    """Run all microservices pattern examples."""
    print("️  Advanced Microservices Patterns with Kafka")
    print("=" * 50)
    print("Examples of event sourcing, CQRS, sagas, and transactional outbox.")
    print()

    try:
        microservices_communication_example()
        saga_pattern_example()
        cqrs_pattern_example()
        transactional_outbox_pattern()

        print("\n All microservices patterns demonstrated!")
        print()
        print(" Key Patterns Covered:")
        print("• Event Sourcing - Store all changes as events")
        print("• CQRS - Separate read and write models")
        print("• Saga - Distributed transaction coordination")
        print("• Transactional Outbox - Reliable event publishing")

    except Exception as e:
        print(f"\n Example failed: {e}")
        print("\n Common Issues:")
        print("• Make sure Kafka is running")
        print("• Check that topics are created automatically")
        print("• Verify network connectivity")


if __name__ == "__main__":
    main()
