/**
 * Basic Consumer Example
 *
 * This example demonstrates the basic usage of the Consumer class
 * to consume messages from a Kafka topic.
 */

import { Config, Consumer } from '../src/index.ts';

async function basicConsumerExample() {
  console.log('🚀 Starting Basic Consumer Example...\n');

  // Create configuration
  const config = new Config({
    bootstrapServers: 'localhost:9092',
    groupId: 'demo-consumer-group',
    autoOffsetReset: 'earliest',
    sessionTimeout: 30000,
  });

  // Create consumer instance
  const consumer = new Consumer(['demo-topic'], config);

  try {
    console.log('📥 Starting to consume messages...');
    console.log('Press Ctrl+C to stop\n');

    let messageCount = 0;
    const maxMessages = 10; // Limit for demo purposes

    while (messageCount < maxMessages) {
      // Poll for a single message
      const message = await consumer.poll(5000);

      if (message) {
        messageCount++;
        console.log(`📨 Message ${messageCount}:`, {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          key: message.key,
          value: message.value,
          timestamp: message.timestamp,
          headers: message.headers,
        });

        // Commit the message offset
        await consumer.commit(message);
        console.log(`✅ Message ${messageCount} committed\n`);
      } else {
        console.log('⏰ No messages received in timeout period');
      }
    }

    console.log(
      `\n🎯 Consumed ${messageCount} messages. Demonstrating batch consumption...\n`,
    );

    // Demonstrate batch consumption
    console.log('📥 Polling for batch of messages...');
    const batchMessages = await consumer.pollBatch(5, 10000);

    if (batchMessages.length > 0) {
      console.log(`📦 Received batch of ${batchMessages.length} messages:`);
      batchMessages.forEach((msg, index) => {
        console.log(`  ${index + 1}. Key: ${msg.key}, Value:`, msg.value);
      });

      // Commit all messages in batch
      await consumer.commit();
      console.log('✅ Batch committed');
    } else {
      console.log('📭 No messages in batch');
    }
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    console.log('\n🔒 Closing consumer...');
    await consumer.close();
    console.log('✅ Consumer closed successfully');
  }
}

// Alternative: Using async iterator
async function consumerAsyncIteratorExample() {
  console.log('🚀 Starting Consumer Async Iterator Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    groupId: 'demo-iterator-group',
    autoOffsetReset: 'earliest',
  });

  const consumer = new Consumer(['demo-topic'], config);

  try {
    console.log('📥 Starting async iteration...');
    let count = 0;

    for await (const message of consumer) {
      count++;
      console.log(`📨 Async Message ${count}:`, {
        key: message.key,
        value: message.value,
        partition: message.partition,
        offset: message.offset,
      });

      // Auto-commit happens internally

      // Stop after 5 messages for demo
      if (count >= 5) {
        break;
      }
    }

    console.log(`✅ Processed ${count} messages via async iterator`);
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await consumer.close();
  }
}

// Run the examples
if (import.meta.main) {
  await basicConsumerExample();
  console.log('\n' + '='.repeat(50) + '\n');
  await consumerAsyncIteratorExample();
}

export { basicConsumerExample, consumerAsyncIteratorExample };
