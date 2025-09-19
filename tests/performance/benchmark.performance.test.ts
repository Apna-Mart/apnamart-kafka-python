import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  Config,
  Consumer,
  Producer,
  TransactionalProducer,
  type MessageInput,
} from '../../src/index.ts';

describe('Performance Benchmark Tests', () => {
  let producer: Producer;
  let consumer: Consumer;
  let testTopic: string;

  beforeEach(() => {
    testTopic = `perf-test-${Math.random().toString(36).substring(7)}-${Date.now()}`;

    const config = new Config({
      bootstrapServers: 'localhost:9092',
      acks: 'all',
      retries: 3,
      batchSize: 16384,
      lingerMs: 5,
      compressionType: 'gzip',
    });

    producer = new Producer(config);
    consumer = new Consumer(
      [testTopic],
      new Config({
        ...config,
        groupId: `perf-test-group-${Math.random().toString(36).substring(7)}`,
        autoOffsetReset: 'earliest',
        fetchMinBytes: 1024,
        fetchMaxWait: 500,
        maxPartitionFetchBytes: 1048576,
      }),
    );
  });

  afterEach(async () => {
    await producer?.close();
    await consumer?.close();
  });

  describe('producer throughput benchmarks', () => {
    it('should achieve >30,000 messages/second producer throughput', async () => {
      const messageCount = 30000;
      const testMessage = { id: 1, content: 'Performance test message', timestamp: Date.now() };

      console.log(`\n📊 Producer Throughput Test: Sending ${messageCount} messages...`);

      const startTime = Date.now();
      const promises = [];

      for (let i = 0; i < messageCount; i++) {
        promises.push(
          producer.send(testTopic, { ...testMessage, id: i })
        );
      }

      await Promise.all(promises);
      const endTime = Date.now();

      const durationMs = endTime - startTime;
      const durationSeconds = durationMs / 1000;
      const throughput = messageCount / durationSeconds;

      console.log(`✅ Sent ${messageCount} messages in ${durationMs}ms`);
      console.log(`🚀 Producer throughput: ${throughput.toFixed(2)} messages/second`);

      expect(throughput).toBeGreaterThan(30000);
      expect(durationMs).toBeLessThan(10000); // Should complete within 10 seconds
    }, 60000);

    it('should achieve optimal batch throughput', async () => {
      const batchSize = 1000;
      const batchCount = 50;
      const totalMessages = batchSize * batchCount;

      console.log(`\n📊 Batch Producer Test: Sending ${batchCount} batches of ${batchSize} messages...`);

      const startTime = Date.now();
      const batchPromises = [];

      for (let b = 0; b < batchCount; b++) {
        const messages: MessageInput[] = [];
        for (let i = 0; i < batchSize; i++) {
          messages.push([
            testTopic,
            { batchId: b, messageId: i, content: `Batch ${b} Message ${i}` },
            `batch-${b}-key-${i}`,
          ]);
        }
        batchPromises.push(producer.sendBatch(messages));
      }

      await Promise.all(batchPromises);
      const endTime = Date.now();

      const durationMs = endTime - startTime;
      const durationSeconds = durationMs / 1000;
      const throughput = totalMessages / durationSeconds;

      console.log(`✅ Sent ${totalMessages} messages in ${batchCount} batches in ${durationMs}ms`);
      console.log(`🚀 Batch throughput: ${throughput.toFixed(2)} messages/second`);

      expect(throughput).toBeGreaterThan(40000); // Batch should be faster than individual sends
    }, 60000);
  });

  describe('consumer throughput benchmarks', () => {
    it('should achieve >25,000 messages/second consumer throughput', async () => {
      const messageCount = 25000;
      const testMessage = { id: 1, content: 'Consumer performance test', timestamp: Date.now() };

      console.log(`\n📊 Consumer Throughput Test: Consuming ${messageCount} messages...`);

      // First, send all messages
      console.log('📤 Sending messages...');
      const sendPromises = [];
      for (let i = 0; i < messageCount; i++) {
        sendPromises.push(
          producer.send(testTopic, { ...testMessage, id: i })
        );
      }
      await Promise.all(sendPromises);

      // Wait for messages to be available
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Now consume them
      console.log('📥 Consuming messages...');
      const startTime = Date.now();
      const consumedMessages = [];

      while (consumedMessages.length < messageCount) {
        const batchSize = Math.min(1000, messageCount - consumedMessages.length);
        const messages = await consumer.pollBatch(batchSize, 10000);

        if (messages.length === 0) {
          break; // No more messages available
        }

        consumedMessages.push(...messages);
      }

      const endTime = Date.now();
      const durationMs = endTime - startTime;
      const durationSeconds = durationMs / 1000;
      const throughput = consumedMessages.length / durationSeconds;

      console.log(`✅ Consumed ${consumedMessages.length} messages in ${durationMs}ms`);
      console.log(`🚀 Consumer throughput: ${throughput.toFixed(2)} messages/second`);

      expect(consumedMessages.length).toBeGreaterThanOrEqual(messageCount * 0.95); // Allow 5% message loss
      expect(throughput).toBeGreaterThan(25000);
    }, 90000);
  });

  describe('latency benchmarks', () => {
    it('should achieve average latency <5ms', async () => {
      const messageCount = 1000;
      const latencies: number[] = [];

      console.log(`\n📊 Latency Test: Measuring ${messageCount} round-trip latencies...`);

      for (let i = 0; i < messageCount; i++) {
        const startTime = Date.now();

        await producer.send(testTopic, {
          id: i,
          content: 'Latency test',
          sendTime: startTime
        });

        // Small delay to avoid overwhelming
        if (i % 100 === 0) {
          await new Promise(resolve => setTimeout(resolve, 10));
        }
      }

      // Wait for messages to be available
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Consume and measure latencies
      const consumedMessages = await consumer.pollBatch(messageCount, 30000);

      for (const message of consumedMessages) {
        const receiveTime = Date.now();
        const sendTime = (message.value as { sendTime: number }).sendTime;
        const latency = receiveTime - sendTime;
        latencies.push(latency);
      }

      const avgLatency = latencies.reduce((sum, lat) => sum + lat, 0) / latencies.length;
      const sortedLatencies = latencies.sort((a, b) => a - b);
      const p95Latency = sortedLatencies[Math.floor(latencies.length * 0.95)];
      const p99Latency = sortedLatencies[Math.floor(latencies.length * 0.99)];

      console.log(`✅ Processed ${latencies.length} messages`);
      console.log(`⚡ Average latency: ${avgLatency.toFixed(2)}ms`);
      console.log(`📈 P95 latency: ${p95Latency}ms`);
      console.log(`📈 P99 latency: ${p99Latency}ms`);

      expect(avgLatency).toBeLessThan(5);
      expect(p95Latency).toBeLessThan(10);
      expect(p99Latency).toBeLessThan(25);
    }, 120000);

    it('should maintain low latency under load', async () => {
      const messageCount = 5000;
      const concurrency = 10;
      const latencies: number[] = [];

      console.log(`\n📊 Load Latency Test: ${messageCount} messages with ${concurrency} concurrent operations...`);

      const promises = [];
      for (let c = 0; c < concurrency; c++) {
        promises.push((async () => {
          for (let i = 0; i < messageCount / concurrency; i++) {
            const startTime = Date.now();
            await producer.send(testTopic, {
              id: c * 1000 + i,
              content: 'Load test',
              sendTime: startTime,
              concurrent: c
            });
          }
        })());
      }

      await Promise.all(promises);

      // Wait for messages to be available
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Consume messages and measure latencies
      const consumedMessages = await consumer.pollBatch(messageCount, 30000);

      for (const message of consumedMessages) {
        const receiveTime = Date.now();
        const sendTime = (message.value as { sendTime: number }).sendTime;
        const latency = receiveTime - sendTime;
        latencies.push(latency);
      }

      const avgLatency = latencies.reduce((sum, lat) => sum + lat, 0) / latencies.length;
      const sortedLatencies = latencies.sort((a, b) => a - b);
      const p95Latency = sortedLatencies[Math.floor(latencies.length * 0.95)];

      console.log(`✅ Load test completed: ${latencies.length} messages processed`);
      console.log(`⚡ Average latency under load: ${avgLatency.toFixed(2)}ms`);
      console.log(`📈 P95 latency under load: ${p95Latency}ms`);

      expect(avgLatency).toBeLessThan(10); // Allow higher latency under load
      expect(p95Latency).toBeLessThan(20);
    }, 120000);
  });

  describe('transactional performance benchmarks', () => {
    it('should maintain reasonable transaction throughput', async () => {
      const transactionCount = 100;
      const messagesPerTransaction = 50;
      const totalMessages = transactionCount * messagesPerTransaction;

      const txProducer = new TransactionalProducer(
        `perf-tx-${Math.random().toString(36).substring(7)}`,
        new Config({
          bootstrapServers: 'localhost:9092',
          acks: 'all',
          retries: 3,
        })
      );

      try {
        console.log(`\n📊 Transaction Performance: ${transactionCount} transactions with ${messagesPerTransaction} messages each...`);

        const startTime = Date.now();

        for (let t = 0; t < transactionCount; t++) {
          await txProducer.begin();

          for (let m = 0; m < messagesPerTransaction; m++) {
            await txProducer.sendTransactional(testTopic, {
              transactionId: t,
              messageId: m,
              content: `TX ${t} Message ${m}`
            });
          }

          await txProducer.commit();
        }

        const endTime = Date.now();
        const durationMs = endTime - startTime;
        const durationSeconds = durationMs / 1000;
        const throughput = totalMessages / durationSeconds;

        console.log(`✅ Completed ${transactionCount} transactions (${totalMessages} messages) in ${durationMs}ms`);
        console.log(`🚀 Transactional throughput: ${throughput.toFixed(2)} messages/second`);

        expect(throughput).toBeGreaterThan(5000); // Transactions are slower but should still be reasonable
      } finally {
        await txProducer.close();
      }
    }, 120000);

    it('should handle batch transactional operations efficiently', async () => {
      const batchCount = 50;
      const messagesPerBatch = 100;
      const totalMessages = batchCount * messagesPerBatch;

      const txProducer = new TransactionalProducer(
        `perf-batch-tx-${Math.random().toString(36).substring(7)}`,
        new Config({
          bootstrapServers: 'localhost:9092',
          acks: 'all',
          retries: 3,
        })
      );

      try {
        console.log(`\n📊 Batch Transaction Performance: ${batchCount} batch transactions with ${messagesPerBatch} messages each...`);

        const startTime = Date.now();

        for (let b = 0; b < batchCount; b++) {
          const messages: MessageInput[] = [];
          for (let m = 0; m < messagesPerBatch; m++) {
            messages.push([
              testTopic,
              { batchId: b, messageId: m, content: `Batch TX ${b} Message ${m}` },
              `batch-tx-${b}-${m}`
            ]);
          }

          await txProducer.sendBatchTransactional(messages);
        }

        const endTime = Date.now();
        const durationMs = endTime - startTime;
        const durationSeconds = durationMs / 1000;
        const throughput = totalMessages / durationSeconds;

        console.log(`✅ Completed ${batchCount} batch transactions (${totalMessages} messages) in ${durationMs}ms`);
        console.log(`🚀 Batch transactional throughput: ${throughput.toFixed(2)} messages/second`);

        expect(throughput).toBeGreaterThan(10000); // Batch transactions should be more efficient
      } finally {
        await txProducer.close();
      }
    }, 120000);
  });

  describe('memory and resource usage benchmarks', () => {
    it('should handle large message volumes without memory leaks', async () => {
      const iterationCount = 10;
      const messagesPerIteration = 1000;

      console.log(`\n📊 Memory Usage Test: ${iterationCount} iterations of ${messagesPerIteration} messages...`);

      const startMemory = process.memoryUsage();

      for (let iteration = 0; iteration < iterationCount; iteration++) {
        console.log(`  Iteration ${iteration + 1}/${iterationCount}`);

        // Send messages
        const sendPromises = [];
        for (let i = 0; i < messagesPerIteration; i++) {
          sendPromises.push(
            producer.send(testTopic, {
              iteration,
              messageId: i,
              data: 'x'.repeat(1000), // 1KB message
              timestamp: Date.now()
            })
          );
        }
        await Promise.all(sendPromises);

        // Consume messages
        await new Promise(resolve => setTimeout(resolve, 500));
        const messages = await consumer.pollBatch(messagesPerIteration, 10000);

        expect(messages.length).toBeGreaterThan(0);

        // Force garbage collection if available
        if (global.gc) {
          global.gc();
        }
      }

      const endMemory = process.memoryUsage();
      const memoryIncrease = endMemory.heapUsed - startMemory.heapUsed;
      const memoryIncreaseMB = memoryIncrease / 1024 / 1024;

      console.log(`📊 Memory usage increase: ${memoryIncreaseMB.toFixed(2)}MB`);
      console.log(`📊 Start heap: ${(startMemory.heapUsed / 1024 / 1024).toFixed(2)}MB`);
      console.log(`📊 End heap: ${(endMemory.heapUsed / 1024 / 1024).toFixed(2)}MB`);

      // Memory increase should be reasonable (less than 100MB for this test)
      expect(memoryIncreaseMB).toBeLessThan(100);
    }, 180000);

    it('should handle rapid connection cycles efficiently', async () => {
      const cycles = 20;

      console.log(`\n📊 Connection Cycle Test: ${cycles} rapid create/close cycles...`);

      const startTime = Date.now();

      for (let i = 0; i < cycles; i++) {
        const tempProducer = new Producer(new Config({
          bootstrapServers: 'localhost:9092',
          acks: 'all',
        }));

        const tempConsumer = new Consumer([testTopic], new Config({
          bootstrapServers: 'localhost:9092',
          groupId: `temp-group-${i}-${Date.now()}`,
          autoOffsetReset: 'latest',
        }));

        // Send one message to establish connection
        await tempProducer.send(testTopic, { cycle: i, test: 'connection' });

        // Try to consume (establishes consumer connection)
        await tempConsumer.poll(100);

        // Close connections
        await tempProducer.close();
        await tempConsumer.close();
      }

      const endTime = Date.now();
      const durationMs = endTime - startTime;
      const avgCycleTime = durationMs / cycles;

      console.log(`✅ Completed ${cycles} connection cycles in ${durationMs}ms`);
      console.log(`⚡ Average cycle time: ${avgCycleTime.toFixed(2)}ms`);

      expect(avgCycleTime).toBeLessThan(1000); // Each cycle should be fast
      expect(durationMs).toBeLessThan(30000); // Total should complete in reasonable time
    }, 120000);
  });

  describe('concurrent operation benchmarks', () => {
    it('should handle high concurrency without degradation', async () => {
      const concurrentProducers = 10;
      const messagesPerProducer = 500;
      const totalMessages = concurrentProducers * messagesPerProducer;

      console.log(`\n📊 Concurrency Test: ${concurrentProducers} concurrent producers, ${messagesPerProducer} messages each...`);

      const producers: Producer[] = [];
      for (let i = 0; i < concurrentProducers; i++) {
        producers.push(new Producer(new Config({
          bootstrapServers: 'localhost:9092',
          acks: 'all',
          clientId: `concurrent-producer-${i}`,
        })));
      }

      try {
        const startTime = Date.now();

        // Start all producers concurrently
        const producerPromises = producers.map((prod, index) =>
          (async () => {
            for (let i = 0; i < messagesPerProducer; i++) {
              await prod.send(testTopic, {
                producerId: index,
                messageId: i,
                content: `Producer ${index} Message ${i}`,
                timestamp: Date.now()
              });
            }
          })()
        );

        await Promise.all(producerPromises);
        const endTime = Date.now();

        const durationMs = endTime - startTime;
        const durationSeconds = durationMs / 1000;
        const throughput = totalMessages / durationSeconds;

        console.log(`✅ Concurrent producers sent ${totalMessages} messages in ${durationMs}ms`);
        console.log(`🚀 Concurrent throughput: ${throughput.toFixed(2)} messages/second`);

        expect(throughput).toBeGreaterThan(20000); // Should maintain good throughput under concurrency
      } finally {
        await Promise.all(producers.map(p => p.close()));
      }
    }, 120000);
  });
});