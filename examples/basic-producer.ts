/**
 * Basic Producer Example
 *
 * This example demonstrates the basic usage of the Producer class
 * to send messages to a Kafka topic.
 */

import { Config, Producer } from '../src/index.ts';

async function basicProducerExample() {
  console.log('🚀 Starting Basic Producer Example...\n');

  // Create configuration
  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
    retries: 3,
    requestTimeout: 5000,
  });

  // Create producer instance
  const producer = new Producer(config);

  try {
    console.log('📤 Sending simple message...');

    // Send a simple message
    const result1 = await producer.send('demo-topic', {
      message: 'Hello, Kafka!',
      timestamp: new Date().toISOString(),
    });

    console.log('✅ Message sent:', result1);

    console.log('\n📤 Sending message with key...');

    // Send message with key for partitioning
    const result2 = await producer.send(
      'demo-topic',
      { userId: 123, action: 'login', ip: '192.168.1.1' },
      'user-123'
    );

    console.log('✅ Message with key sent:', result2);

    console.log('\n📤 Sending message with headers and options...');

    // Send message with headers and specific partition
    const result3 = await producer.send(
      'demo-topic',
      { orderId: 'ORD-001', amount: 99.99, currency: 'USD' },
      'order-ORD-001',
      {
        headers: {
          'content-type': 'application/json',
          'source': 'payment-service',
          'version': '1.0',
        },
        partition: 0,
      }
    );

    console.log('✅ Message with headers sent:', result3);

    console.log('\n📤 Sending different data types...');

    // String message
    await producer.send('demo-topic', 'Plain string message');

    // Number message
    await producer.send('demo-topic', 42);

    // Boolean message
    await producer.send('demo-topic', true);

    // Array message
    await producer.send('demo-topic', [1, 2, 3, 'array', { nested: true }]);

    console.log('✅ Various data types sent successfully');

    console.log('\n🔄 Flushing producer to ensure delivery...');
    await producer.flush();
    console.log('✅ All messages flushed');

  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    console.log('\n🔒 Closing producer...');
    await producer.close();
    console.log('✅ Producer closed successfully');
  }
}

// Run the example
if (import.meta.main) {
  basicProducerExample().catch(console.error);
}

export { basicProducerExample };