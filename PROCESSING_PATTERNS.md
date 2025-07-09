# Implementing Processing Patterns with `apnamart-kafka-python`

This document explains how to implement common Kafka processing patterns—specifically, strict sequential processing and massively parallel processing—using this library.

The key takeaway is that the library's `KafkaConsumer` and `KafkaProducer` are already equipped to handle these scenarios. The control comes not from complex application code, but from two of Kafka's fundamental concepts:
1.  **Topic Partitions**: The unit of parallelism in Kafka.
2.  **Consumer Groups**: How multiple consumer instances cooperate to process messages.

---

## Scenario 1: Strict Sequential Processing

**Goal:** Process every message in a topic in the exact order it was produced. This is critical for use cases like financial ledgers, event sourcing, or any state machine where order is paramount.

### How to Implement

#### 1. Topic Configuration: Use a Single Partition

You must create the topic with **exactly one partition**. This enforces a single, ordered log for all messages.

```sh
# Use the kafka-topics.sh script from your Kafka installation
kafka-topics.sh --create \
  --topic sequential_orders \
  --bootstrap-server <your_broker_url:9092> \
  --partitions 1 \
  --replication-factor 3 # Or your desired replication factor
```

#### 2. Consumer Implementation

Use the standard `KafkaConsumer` and subscribe it to `sequential_orders`. Assign it a `group_id`.

Even if you run multiple instances of your consumer application with the same `group_id`, Kafka will guarantee that **only one instance is assigned the single partition** at any given time. The other instances will remain idle (for this topic), acting as hot standbys.

---

## Scenario 2: Parallel Processing (High Throughput)

**Goal:** Process a high volume of messages from a topic as quickly as possible by distributing the load across multiple consumer instances.

### How to Implement

#### 1. Topic Configuration: Use Multiple Partitions

Create the topic with **multiple partitions**. The number of partitions determines the *maximum* level of parallelism you can achieve. A good starting point is often 6, 12, or more, depending on your expected load.

```sh
# Create a topic with 12 partitions
kafka-topics.sh --create \
  --topic parallel_iot_events \
  --bootstrap-server <your_broker_url:9092> \
  --partitions 12 \
  --replication-factor 3
```

#### 2. Consumer Implementation

Use the standard `KafkaConsumer` with a consistent `group_id`. When you run multiple instances of this consumer:
- Kafka automatically distributes the 12 partitions among the active consumers in the group.
- If you run 3 instances, each will be assigned 4 partitions.
- If you run 6 instances, each will be assigned 2 partitions.
- If you run 12 instances, each will be assigned 1 partition.
- If you run 13 instances, the 13th will be idle.

This allows your application to scale horizontally to meet demand.

---

## Combined Scenario: Running Both Patterns in One Service

You can easily manage both patterns within a single logical service. The consumer application subscribes to both the sequential and parallel topics. Kafka's consumer group protocol handles the rest.

An example implementation, including a producer and a consumer that demonstrates this combined pattern, has been added to `examples/processing_patterns_demo.py`.

### How to Run the Example

1.  **Create the Topics:**
    ```sh
    # Sequential Topic
    kafka-topics.sh --create --topic sequential_topic --bootstrap-server localhost:9092 --partitions 1
    # Parallel Topic
    kafka-topics.sh --create --topic parallel_topic --bootstrap-server localhost:9092 --partitions 12
    ```

2.  **Run the Producer (once):**
    This will populate the topics with some sample messages.
    ```sh
    python examples/processing_patterns_demo.py --role producer
    ```

3.  **Run the Consumers:**
    Open several terminal windows and run the same command in each.
    ```sh
    # In Terminal 1
    python examples/processing_patterns_demo.py --role consumer

    # In Terminal 2
    python examples/processing_patterns_demo.py --role consumer

    # In Terminal 3
    python examples/processing_patterns_demo.py --role consumer
    ```

You will observe that:
- Messages from `sequential_topic` are **always processed by only one** of the running consumer instances.
- Messages from `parallel_topic` are **distributed across all** the consumer instances, showing parallel processing in action.

---

## Advanced Scenario: Parallel Processing Within a Single Instance

It is also possible to process messages concurrently within a *single* consumer application by using a thread pool. This is an advanced pattern best suited for I/O-bound workloads where you want to maximize the resources of a single machine.

### How it Works

The consumer polls a batch of messages, which may come from multiple partitions assigned to it. Instead of processing them sequentially in the main thread, it submits each message to a `ThreadPoolExecutor` for concurrent processing.

An example of this pattern can be found in the `run_consumer_parallel_internal` function inside `examples/processing_patterns_demo.py`.

### Critical Trade-offs and Considerations

**1. Increased Complexity:** You are responsible for managing threads, which can introduce race conditions and requires careful code design.

**2. Not for CPU-Bound Work:** Due to Python's Global Interpreter Lock (GIL), this pattern provides significant benefits only for **I/O-bound** tasks (e.g., making API calls, querying a database). It will not speed up CPU-bound work.

**3. Manual Offset Management is Required:** Using `enable_auto_commit=True` is **highly dangerous** in this model. The consumer may commit a message's offset *before* its processing thread is complete. If the application crashes, the message is lost. For a production-ready implementation, you **must** disable auto-commit and manually commit offsets only after a message has been successfully processed by its thread. This adds significant complexity.

**4. Reduced Fault Tolerance:** The standard multi-process model is more resilient. If a single threaded consumer process crashes, all of its assigned partitions stop processing until the consumer restarts. In the multi-process model, Kafka automatically reassigns the partitions to the other healthy instances.

### Recommendation

For most use cases, the standard model of running **multiple consumer processes** is the recommended approach. It is simpler, more robust, and leverages Kafka's native fault tolerance. Use the single-instance, multi-threaded pattern only when you have a specific need to maximize I/O concurrency on a single node and are prepared to handle the additional complexity.

