/**
 * Batch Operations Example
 *
 * This example demonstrates how to efficiently send and receive
 * multiple messages using batch operations.
 */

import { Config, Consumer, type MessageInput, Producer } from '../src/index.ts';

async function batchOperationsExample() {
  console.log('🚀 Starting Batch Operations Example...\n');

  // Create optimized configuration for batch operations
  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
    retries: 3,
    batchSize: 16384, // 16KB batch size
    lingerMs: 10, // Wait 10ms to collect more messages
    compressionType: 'gzip', // Compress batches
  });

  const producer = new Producer(config);
  const consumer = new Consumer(
    ['batch-demo-topic'],
    new Config({
      ...config,
      groupId: 'batch-demo-group',
      autoOffsetReset: 'earliest',
      fetchMinBytes: 1024, // Fetch at least 1KB
      fetchMaxWait: 500, // Wait max 500ms for fetch
    }),
  );

  try {
    console.log('📤 Preparing batch messages...');

    // Prepare batch messages using array format
    const arrayMessages: MessageInput[] = [
      ['batch-demo-topic', { id: 1, type: 'order', amount: 99.99 }],
      [
        'batch-demo-topic',
        { id: 2, type: 'payment', amount: 149.99 },
        'payment-key',
      ],
      [
        'batch-demo-topic',
        { id: 3, type: 'shipment', tracking: 'TRK123' },
        'ship-key',
      ],
    ];

    // Prepare batch messages using object format
    const objectMessages: MessageInput[] = [
      {
        topic: 'batch-demo-topic',
        value: { id: 4, type: 'notification', message: 'Order confirmed' },
        key: 'notif-1',
        headers: { 'content-type': 'application/json', priority: 'high' },
      },
      {
        topic: 'batch-demo-topic',
        value: { id: 5, type: 'analytics', event: 'page_view' },
        key: 'analytics-1',
        partition: 0,
      },
      {
        topic: 'batch-demo-topic',
        value: { id: 6, type: 'log', level: 'info', message: 'User logged in' },
      },
    ];

    // Mix both formats
    const mixedMessages: MessageInput[] = [...arrayMessages, ...objectMessages];

    console.log(`📦 Sending batch of ${mixedMessages.length} messages...`);

    const startTime = Date.now();
    const results = await producer.sendBatch(mixedMessages);
    const endTime = Date.now();

    console.log(`✅ Batch sent in ${endTime - startTime}ms`);
    console.log('📊 Results summary:');

    const successful = results.filter((r) => r.success).length;
    const failed = results.filter((r) => !r.success).length;

    console.log(`  ✅ Successful: ${successful}`);
    console.log(`  ❌ Failed: ${failed}`);

    if (failed > 0) {
      console.log('  Failed messages:');
      results.forEach((result, index) => {
        if (!result.success) {
          console.log(`    ${index}: ${result.error}`);
        }
      });
    }

    console.log('\n📥 Waiting for messages to be available...');
    await new Promise((resolve) => setTimeout(resolve, 2000));

    console.log('📦 Consuming messages in batch...');
    const consumedMessages = await consumer.pollBatch(
      mixedMessages.length,
      15000,
    );

    console.log(`📨 Received ${consumedMessages.length} messages:`);
    consumedMessages.forEach((msg, index) => {
      console.log(
        `  ${index + 1}. [${msg.partition}:${msg.offset}] Key: ${msg.key}, Type: ${(msg.value as { type: string }).type}`,
      );
    });

    // Demonstrate large batch performance
    await demonstrateLargeBatchPerformance(producer, consumer);
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    console.log('\n🔒 Closing connections...');
    await producer.close();
    await consumer.close();
    console.log('✅ Connections closed');
  }
}

async function demonstrateLargeBatchPerformance(
  producer: Producer,
  consumer: Consumer,
) {
  console.log('\n' + '='.repeat(50));
  console.log('🚀 Large Batch Performance Demo\n');

  const batchSize = 1000;
  const messages: MessageInput[] = [];

  // Generate large batch
  console.log(`📦 Generating ${batchSize} messages...`);
  for (let i = 0; i < batchSize; i++) {
    messages.push([
      'batch-demo-topic',
      {
        id: i,
        timestamp: Date.now(),
        data: `Message ${i}`,
        random: Math.random(),
      },
      `key-${i}`,
    ]);
  }

  // Send large batch
  console.log(`📤 Sending large batch of ${batchSize} messages...`);
  const startTime = Date.now();
  const results = await producer.sendBatch(messages);
  const endTime = Date.now();

  const duration = endTime - startTime;
  const throughput = (batchSize / duration) * 1000; // messages per second

  console.log(`✅ Large batch sent successfully!`);
  console.log(`⏱️  Duration: ${duration}ms`);
  console.log(`🚀 Throughput: ${throughput.toFixed(2)} messages/second`);

  const successful = results.filter((r) => r.success).length;
  console.log(
    `✅ Success rate: ${successful}/${batchSize} (${((successful / batchSize) * 100).toFixed(1)}%)`,
  );

  // Wait and consume
  console.log('\n📥 Waiting for messages to be available...');
  await new Promise((resolve) => setTimeout(resolve, 3000));

  console.log('📦 Consuming large batch...');
  const consumeStartTime = Date.now();
  const consumedMessages = await consumer.pollBatch(batchSize, 30000);
  const consumeEndTime = Date.now();

  const consumeDuration = consumeEndTime - consumeStartTime;
  const consumeThroughput = (consumedMessages.length / consumeDuration) * 1000;

  console.log(`📨 Consumed ${consumedMessages.length} messages`);
  console.log(`⏱️  Consume duration: ${consumeDuration}ms`);
  console.log(
    `📥 Consume throughput: ${consumeThroughput.toFixed(2)} messages/second`,
  );
}

// Demonstrate multi-topic batch operations
async function multiTopicBatchExample() {
  console.log('🚀 Starting Multi-Topic Batch Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const producer = new Producer(config);

  try {
    // Messages for different topics
    const multiTopicMessages: MessageInput[] = [
      ['orders-topic', { orderId: 'ORD-001', amount: 99.99 }],
      [
        'payments-topic',
        { paymentId: 'PAY-001', orderId: 'ORD-001', amount: 99.99 },
      ],
      ['inventory-topic', { productId: 'PROD-001', quantity: -1 }],
      ['notifications-topic', { userId: 123, message: 'Order placed' }],
      [
        'analytics-topic',
        { event: 'order_placed', userId: 123, amount: 99.99 },
      ],
    ];

    console.log('📤 Sending messages to multiple topics...');
    const results = await producer.sendBatch(multiTopicMessages);

    console.log('📊 Multi-topic batch results:');
    results.forEach((result, index) => {
      const message = multiTopicMessages[index];
      const topic = Array.isArray(message) ? message[0] : message.topic;
      console.log(`  ${topic}: ${result.success ? '✅' : '❌'}`);
    });
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await producer.close();
  }
}

// Run the examples
if (import.meta.main) {
  await batchOperationsExample();
  console.log('\n' + '='.repeat(60) + '\n');
  await multiTopicBatchExample();
}

export { batchOperationsExample, multiTopicBatchExample };
