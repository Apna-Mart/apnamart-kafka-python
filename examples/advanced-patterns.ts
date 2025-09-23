/**
 * Advanced Kafka Patterns Example
 *
 * This example demonstrates advanced patterns like:
 * - Dead Letter Queue (DLQ)
 * - Retry mechanisms
 * - Message deduplication
 * - Partitioning strategies
 * - Consumer group coordination
 */

import { Config, Consumer, type MessageInput, Producer } from '../src/index.ts';

// Dead Letter Queue Pattern
async function deadLetterQueueExample() {
  console.log('🚀 Starting Dead Letter Queue Pattern Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
    retries: 3,
  });

  const producer = new Producer(config);
  const mainConsumer = new Consumer(
    ['main-topic'],
    new Config({
      ...config,
      groupId: 'main-processor-group',
      autoOffsetReset: 'earliest',
    }),
  );

  const dlqConsumer = new Consumer(
    ['dlq-topic'],
    new Config({
      ...config,
      groupId: 'dlq-processor-group',
      autoOffsetReset: 'earliest',
    }),
  );

  try {
    console.log('📤 Sending test messages...');

    // Send some messages - some will "fail" processing
    const messages = [
      { id: 1, data: 'valid-data', type: 'order' },
      { id: 2, data: 'invalid-format', type: 'invalid' }, // This will "fail"
      { id: 3, data: 'valid-data', type: 'payment' },
      { id: 4, data: 'corrupt-data', type: 'invalid' }, // This will "fail"
      { id: 5, data: 'valid-data', type: 'shipment' },
    ];

    for (const message of messages) {
      await producer.send('main-topic', message, `key-${message.id}`);
    }

    console.log('📥 Processing messages with DLQ pattern...');

    // Process messages with retry and DLQ logic
    const processedMessages = [];
    const failedMessages = [];

    for (let i = 0; i < messages.length; i++) {
      const message = await mainConsumer.poll(5000);
      if (!message) continue;

      const messageData = message.value as {
        id: number;
        data: string;
        type: string;
      };

      // Simulate processing logic
      const success = await processMessageWithRetry(
        messageData,
        producer,
        message.key,
      );

      if (success) {
        processedMessages.push(messageData);
        await mainConsumer.commit(message);
        console.log(`✅ Successfully processed message ${messageData.id}`);
      } else {
        failedMessages.push(messageData);

        // Send to DLQ
        await producer.send(
          'dlq-topic',
          {
            ...messageData,
            originalTopic: 'main-topic',
            failureReason: 'Processing failed after retries',
            failedAt: new Date().toISOString(),
          },
          message.key,
        );

        await mainConsumer.commit(message);
        console.log(`🚫 Moved message ${messageData.id} to DLQ`);
      }
    }

    console.log(`\n📊 Processing Summary:`);
    console.log(`  ✅ Processed: ${processedMessages.length}`);
    console.log(`  🚫 Failed (moved to DLQ): ${failedMessages.length}`);

    // Check DLQ
    console.log('\n📭 Checking DLQ...');
    await new Promise((resolve) => setTimeout(resolve, 1000));
    const dlqMessages = await dlqConsumer.pollBatch(10, 5000);
    console.log(`📬 DLQ contains ${dlqMessages.length} failed messages`);
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await producer.close();
    await mainConsumer.close();
    await dlqConsumer.close();
  }
}

// Simulate message processing with retries
async function processMessageWithRetry(
  message: { id: number; data: string; type: string },
  producer: Producer,
  key: string | null,
  maxRetries = 3,
): Promise<boolean> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // Simulate processing logic
      if (message.type === 'invalid') {
        throw new Error(`Invalid message type: ${message.type}`);
      }

      // Simulate occasional failures
      if (Math.random() < 0.1 && attempt === 1) {
        // 10% chance of initial failure
        throw new Error('Temporary processing failure');
      }

      // Simulate processing time
      await new Promise((resolve) => setTimeout(resolve, 100));

      return true; // Success
    } catch (error) {
      console.log(
        `  🔄 Retry ${attempt}/${maxRetries} for message ${message.id}: ${error.message}`,
      );

      if (attempt < maxRetries) {
        // Exponential backoff
        const delay = 2 ** (attempt - 1) * 1000;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  return false; // Failed after all retries
}

// Message Deduplication Pattern
async function messageDeduplicationExample() {
  console.log('\n🚀 Starting Message Deduplication Pattern Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const producer = new Producer(config);
  const consumer = new Consumer(
    ['dedup-topic'],
    new Config({
      ...config,
      groupId: 'dedup-processor-group',
      autoOffsetReset: 'earliest',
    }),
  );

  // Simple in-memory deduplication store (in production, use Redis or database)
  const processedMessageIds = new Set<string>();

  try {
    console.log('📤 Sending duplicate messages...');

    // Send messages with some duplicates
    const messages = [
      { id: 'MSG-001', data: 'First message' },
      { id: 'MSG-002', data: 'Second message' },
      { id: 'MSG-001', data: 'First message (duplicate)' }, // Duplicate
      { id: 'MSG-003', data: 'Third message' },
      { id: 'MSG-002', data: 'Second message (duplicate)' }, // Duplicate
      { id: 'MSG-004', data: 'Fourth message' },
    ];

    for (const message of messages) {
      await producer.send('dedup-topic', message, message.id, {
        headers: {
          'idempotency-key': message.id,
          timestamp: Date.now().toString(),
        },
      });
    }

    console.log('📥 Processing with deduplication...');

    let processedCount = 0;
    let duplicateCount = 0;

    for (let i = 0; i < messages.length; i++) {
      const message = await consumer.poll(5000);
      if (!message) continue;

      const messageData = message.value as { id: string; data: string };
      const idempotencyKey = message.headers['idempotency-key'];

      if (processedMessageIds.has(idempotencyKey)) {
        duplicateCount++;
        console.log(`🔄 Skipping duplicate message: ${messageData.id}`);
      } else {
        processedMessageIds.add(idempotencyKey);
        processedCount++;
        console.log(`✅ Processing new message: ${messageData.id}`);

        // Simulate processing
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      await consumer.commit(message);
    }

    console.log(`\n📊 Deduplication Summary:`);
    console.log(`  ✅ Unique messages processed: ${processedCount}`);
    console.log(`  🔄 Duplicates skipped: ${duplicateCount}`);
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await producer.close();
    await consumer.close();
  }
}

// Partitioning Strategy Example
async function partitioningStrategyExample() {
  console.log('\n🚀 Starting Partitioning Strategy Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const producer = new Producer(config);

  try {
    console.log('📤 Demonstrating different partitioning strategies...');

    // Strategy 1: Key-based partitioning (default)
    console.log('\n1️⃣ Key-based partitioning:');
    const userMessages = [
      { userId: 'user-1', action: 'login' },
      { userId: 'user-2', action: 'purchase' },
      { userId: 'user-1', action: 'logout' }, // Same user, same partition
      { userId: 'user-3', action: 'login' },
      { userId: 'user-2', action: 'logout' }, // Same user, same partition
    ];

    for (const message of userMessages) {
      const result = await producer.send(
        'partition-demo-topic',
        message,
        message.userId,
      );
      console.log(`  User ${message.userId} -> Partition ${result.partition}`);
    }

    // Strategy 2: Round-robin partitioning (no key)
    console.log('\n2️⃣ Round-robin partitioning (no key):');
    for (let i = 0; i < 5; i++) {
      const message = { id: i, data: `Round-robin message ${i}` };
      const result = await producer.send('partition-demo-topic', message);
      console.log(`  Message ${i} -> Partition ${result.partition}`);
    }

    // Strategy 3: Custom partitioning by message type
    console.log('\n3️⃣ Custom partitioning by message type:');
    const typedMessages = [
      { type: 'order', data: 'Order 1' },
      { type: 'payment', data: 'Payment 1' },
      { type: 'order', data: 'Order 2' },
      { type: 'shipment', data: 'Shipment 1' },
      { type: 'payment', data: 'Payment 2' },
    ];

    for (const message of typedMessages) {
      // Use message type as key for consistent partitioning by type
      const result = await producer.send(
        'partition-demo-topic',
        message,
        message.type,
      );
      console.log(`  ${message.type} message -> Partition ${result.partition}`);
    }

    // Strategy 4: Explicit partition selection
    console.log('\n4️⃣ Explicit partition selection:');
    for (let partition = 0; partition < 3; partition++) {
      const message = { explicit: true, targetPartition: partition };
      const result = await producer.send(
        'partition-demo-topic',
        message,
        null,
        { partition },
      );
      console.log(
        `  Explicit partition ${partition} -> Actually sent to ${result.partition}`,
      );
    }
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await producer.close();
  }
}

// Consumer Group Coordination Example
async function consumerGroupCoordinationExample() {
  console.log('\n🚀 Starting Consumer Group Coordination Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const producer = new Producer(config);

  // Create multiple consumers in the same group
  const groupId = `coordination-group-${Date.now()}`;
  const consumer1 = new Consumer(
    ['coordination-topic'],
    new Config({
      ...config,
      groupId,
      autoOffsetReset: 'earliest',
      sessionTimeout: 30000,
    }),
  );

  const consumer2 = new Consumer(
    ['coordination-topic'],
    new Config({
      ...config,
      groupId,
      autoOffsetReset: 'earliest',
      sessionTimeout: 30000,
    }),
  );

  try {
    console.log('📤 Sending messages for group coordination...');

    // Send messages to multiple partitions
    const messages = [];
    for (let i = 0; i < 20; i++) {
      const message = {
        id: i,
        data: `Coordination message ${i}`,
        timestamp: new Date().toISOString(),
      };
      messages.push(message);

      // Use different keys to distribute across partitions
      await producer.send('coordination-topic', message, `key-${i % 4}`);
    }

    console.log('📥 Starting coordinated consumption...');

    // Start both consumers
    const consumer1Promise = consumeWithId(consumer1, 'Consumer-1', 15);
    const consumer2Promise = consumeWithId(consumer2, 'Consumer-2', 15);

    // Wait for both consumers to process messages
    const [consumer1Results, consumer2Results] = await Promise.all([
      consumer1Promise,
      consumer2Promise,
    ]);

    console.log(`\n📊 Group Coordination Results:`);
    console.log(`  Consumer-1 processed: ${consumer1Results.length} messages`);
    console.log(`  Consumer-2 processed: ${consumer2Results.length} messages`);
    console.log(
      `  Total processed: ${consumer1Results.length + consumer2Results.length}`,
    );

    // Check for overlaps (shouldn't happen with proper coordination)
    const consumer1Ids = new Set(consumer1Results.map((m) => m.id));
    const consumer2Ids = new Set(consumer2Results.map((m) => m.id));
    const overlaps = consumer1Results.filter((m) => consumer2Ids.has(m.id));

    console.log(`  Overlapping messages: ${overlaps.length} (should be 0)`);
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await producer.close();
    await consumer1.close();
    await consumer2.close();
  }
}

async function consumeWithId(
  consumer: Consumer,
  consumerId: string,
  maxMessages: number,
): Promise<Array<{ id: number; data: string }>> {
  const results: Array<{ id: number; data: string }> = [];
  let attempts = 0;
  const maxAttempts = 30; // Prevent infinite loops

  while (results.length < maxMessages && attempts < maxAttempts) {
    attempts++;
    const message = await consumer.poll(2000);

    if (message) {
      const messageData = message.value as { id: number; data: string };
      results.push(messageData);
      console.log(
        `  ${consumerId}: Processed message ${messageData.id} from partition ${message.partition}`,
      );
      await consumer.commit(message);
    }
  }

  return results;
}

// Event Sourcing Pattern Example
async function eventSourcingExample() {
  console.log('\n🚀 Starting Event Sourcing Pattern Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const producer = new Producer(config);

  try {
    console.log('📤 Publishing event stream...');

    // Simulate an order lifecycle through events
    const orderId = `ORDER-${Date.now()}`;
    const events = [
      {
        eventType: 'OrderCreated',
        aggregateId: orderId,
        eventId: `${orderId}-001`,
        timestamp: new Date().toISOString(),
        data: {
          customerId: 'CUST-001',
          items: [{ productId: 'PROD-001', quantity: 2, price: 49.99 }],
          total: 99.98,
        },
      },
      {
        eventType: 'PaymentProcessed',
        aggregateId: orderId,
        eventId: `${orderId}-002`,
        timestamp: new Date().toISOString(),
        data: {
          paymentMethod: 'credit_card',
          amount: 99.98,
          transactionId: 'TXN-001',
        },
      },
      {
        eventType: 'InventoryReserved',
        aggregateId: orderId,
        eventId: `${orderId}-003`,
        timestamp: new Date().toISOString(),
        data: {
          items: [
            { productId: 'PROD-001', quantity: 2, reservationId: 'RES-001' },
          ],
        },
      },
      {
        eventType: 'OrderShipped',
        aggregateId: orderId,
        eventId: `${orderId}-004`,
        timestamp: new Date().toISOString(),
        data: {
          trackingNumber: 'TRACK-001',
          carrier: 'DHL',
          estimatedDelivery: new Date(
            Date.now() + 3 * 24 * 60 * 60 * 1000,
          ).toISOString(),
        },
      },
    ];

    // Publish events in order
    for (const event of events) {
      await producer.send('events-topic', event, event.aggregateId, {
        headers: {
          'event-type': event.eventType,
          'aggregate-id': event.aggregateId,
          'event-id': event.eventId,
          'event-version': '1',
        },
      });

      console.log(
        `  📝 Published: ${event.eventType} for ${event.aggregateId}`,
      );

      // Small delay to ensure ordering
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log(`\n✅ Event stream published for order ${orderId}`);

    // Demonstrate event replay/reconstruction
    const consumer = new Consumer(
      ['events-topic'],
      new Config({
        ...config,
        groupId: `event-replay-${Date.now()}`,
        autoOffsetReset: 'earliest',
      }),
    );

    console.log('\n📖 Replaying events to reconstruct state...');

    const orderState = {
      orderId: null,
      status: 'unknown',
      customer: null,
      items: [],
      total: 0,
      payment: null,
      inventory: null,
      shipping: null,
    };

    // Read and apply events
    for (let i = 0; i < events.length; i++) {
      const message = await consumer.poll(5000);
      if (!message) continue;

      const event = message.value as any;

      // Apply event to state
      switch (event.eventType) {
        case 'OrderCreated':
          orderState.orderId = event.aggregateId;
          orderState.status = 'created';
          orderState.customer = event.data.customerId;
          orderState.items = event.data.items;
          orderState.total = event.data.total;
          break;
        case 'PaymentProcessed':
          orderState.status = 'paid';
          orderState.payment = event.data;
          break;
        case 'InventoryReserved':
          orderState.status = 'reserved';
          orderState.inventory = event.data;
          break;
        case 'OrderShipped':
          orderState.status = 'shipped';
          orderState.shipping = event.data;
          break;
      }

      console.log(
        `  🔄 Applied ${event.eventType} -> Status: ${orderState.status}`,
      );
    }

    console.log('\n📊 Final reconstructed state:', orderState);

    await consumer.close();
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await producer.close();
  }
}

// Run all examples
if (import.meta.main) {
  console.log('🎯 Running Advanced Kafka Patterns Examples\n');
  console.log('='.repeat(80) + '\n');

  await deadLetterQueueExample();
  console.log('\n' + '='.repeat(80) + '\n');

  await messageDeduplicationExample();
  console.log('\n' + '='.repeat(80) + '\n');

  await partitioningStrategyExample();
  console.log('\n' + '='.repeat(80) + '\n');

  await consumerGroupCoordinationExample();
  console.log('\n' + '='.repeat(80) + '\n');

  await eventSourcingExample();

  console.log('\n🎉 All advanced pattern examples completed!');
}

export {
  deadLetterQueueExample,
  messageDeduplicationExample,
  partitioningStrategyExample,
  consumerGroupCoordinationExample,
  eventSourcingExample,
};
