/**
 * Transactional Producer Example
 *
 * This example demonstrates how to use the TransactionalProducer
 * for atomic message operations across multiple topics.
 */

import { Config, TransactionalProducer, Consumer, type MessageInput } from '../src/index.ts';

async function basicTransactionExample() {
  console.log('🚀 Starting Basic Transaction Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
    retries: 3,
  });

  const transactionalId = `tx-demo-${Date.now()}`;
  const txProducer = new TransactionalProducer(transactionalId, config);

  const consumer = new Consumer(
    ['tx-demo-topic'],
    new Config({
      ...config,
      groupId: 'tx-demo-group',
      autoOffsetReset: 'earliest',
    })
  );

  try {
    console.log('💰 Starting successful transaction...');

    // Begin transaction
    await txProducer.begin();
    console.log('✅ Transaction begun');

    // Send messages within transaction
    await txProducer.sendTransactional('tx-demo-topic', {
      orderId: 'ORD-001',
      status: 'created',
      timestamp: new Date().toISOString(),
    });

    await txProducer.sendTransactional('tx-demo-topic', {
      orderId: 'ORD-001',
      status: 'paid',
      amount: 99.99,
      timestamp: new Date().toISOString(),
    });

    await txProducer.sendTransactional('tx-demo-topic', {
      orderId: 'ORD-001',
      status: 'confirmed',
      timestamp: new Date().toISOString(),
    });

    // Commit transaction
    await txProducer.commit();
    console.log('✅ Transaction committed successfully');

    // Verify messages are visible
    await new Promise(resolve => setTimeout(resolve, 1000));
    const messages = await consumer.pollBatch(3, 10000);
    console.log(`📨 Received ${messages.length} committed messages`);

    console.log('\n💸 Demonstrating aborted transaction...');

    // Begin another transaction
    await txProducer.begin();
    console.log('✅ Second transaction begun');

    // Send messages
    await txProducer.sendTransactional('tx-demo-topic', {
      orderId: 'ORD-002',
      status: 'created',
      timestamp: new Date().toISOString(),
    });

    await txProducer.sendTransactional('tx-demo-topic', {
      orderId: 'ORD-002',
      status: 'payment_failed',
      timestamp: new Date().toISOString(),
    });

    // Abort transaction
    await txProducer.abort();
    console.log('🚫 Transaction aborted');

    // Verify messages are NOT visible
    await new Promise(resolve => setTimeout(resolve, 1000));
    const abortedMessages = await consumer.pollBatch(2, 2000);
    console.log(`📭 Received ${abortedMessages.length} messages after abort (should be 0)`);

  } catch (error) {
    console.error('❌ Error:', error);
    // In case of error, try to abort
    try {
      await txProducer.abort();
      console.log('🚫 Transaction aborted due to error');
    } catch (abortError) {
      console.error('❌ Failed to abort transaction:', abortError);
    }
  } finally {
    await txProducer.close();
    await consumer.close();
  }
}

async function batchTransactionExample() {
  console.log('\n🚀 Starting Batch Transaction Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const transactionalId = `batch-tx-demo-${Date.now()}`;
  const txProducer = new TransactionalProducer(transactionalId, config);

  try {
    console.log('📦 Sending batch within single transaction...');

    const batchMessages: MessageInput[] = [
      ['tx-demo-topic', { id: 1, type: 'user_registration', userId: 'user123' }],
      ['tx-demo-topic', { id: 2, type: 'profile_created', userId: 'user123' }, 'user123'],
      {
        topic: 'tx-demo-topic',
        value: { id: 3, type: 'welcome_email_sent', userId: 'user123' },
        key: 'user123',
        headers: { 'email-type': 'welcome' },
      },
      ['tx-demo-topic', { id: 4, type: 'onboarding_started', userId: 'user123' }],
    ];

    // Send entire batch as one transaction
    await txProducer.sendBatchTransactional(batchMessages);
    console.log('✅ Batch transaction completed successfully');

    console.log('\n🚫 Demonstrating batch transaction failure...');

    // Demonstrate batch with failure
    const failingBatch: MessageInput[] = [
      ['tx-demo-topic', { id: 5, type: 'valid_message' }],
      'invalid-message-format' as unknown as MessageInput, // This will cause failure
      ['tx-demo-topic', { id: 6, type: 'another_valid_message' }],
    ];

    try {
      await txProducer.sendBatchTransactional(failingBatch);
    } catch (error) {
      console.log('🚫 Batch transaction failed as expected:', error.message);
    }

  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await txProducer.close();
  }
}

async function crossTopicTransactionExample() {
  console.log('\n🚀 Starting Cross-Topic Transaction Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
  });

  const transactionalId = `cross-topic-tx-${Date.now()}`;
  const txProducer = new TransactionalProducer(transactionalId, config);

  // Create consumers for different topics
  const orderConsumer = new Consumer(['orders-topic'], new Config({
    ...config,
    groupId: 'order-consumer-group',
    autoOffsetReset: 'latest',
  }));

  const paymentConsumer = new Consumer(['payments-topic'], new Config({
    ...config,
    groupId: 'payment-consumer-group',
    autoOffsetReset: 'latest',
  }));

  const inventoryConsumer = new Consumer(['inventory-topic'], new Config({
    ...config,
    groupId: 'inventory-consumer-group',
    autoOffsetReset: 'latest',
  }));

  try {
    console.log('🔄 Executing cross-topic transaction...');

    await txProducer.begin();

    // Simulate an e-commerce order transaction across multiple topics
    const orderId = `ORD-${Date.now()}`;

    // 1. Create order
    await txProducer.sendTransactional('orders-topic', {
      orderId,
      userId: 'user123',
      items: [{ productId: 'PROD-001', quantity: 2, price: 49.99 }],
      total: 99.98,
      status: 'created',
      timestamp: new Date().toISOString(),
    }, orderId);

    // 2. Process payment
    await txProducer.sendTransactional('payments-topic', {
      paymentId: `PAY-${Date.now()}`,
      orderId,
      amount: 99.98,
      method: 'credit_card',
      status: 'processed',
      timestamp: new Date().toISOString(),
    }, orderId);

    // 3. Update inventory
    await txProducer.sendTransactional('inventory-topic', {
      productId: 'PROD-001',
      operation: 'decrement',
      quantity: 2,
      orderId,
      timestamp: new Date().toISOString(),
    }, 'PROD-001');

    // 4. Send notification (different key to distribute load)
    await txProducer.sendTransactional('notifications-topic', {
      userId: 'user123',
      type: 'order_confirmation',
      orderId,
      message: `Your order ${orderId} has been confirmed!`,
      timestamp: new Date().toISOString(),
    }, 'user123');

    await txProducer.commit();
    console.log('✅ Cross-topic transaction committed successfully');

    // Verify messages across topics
    await new Promise(resolve => setTimeout(resolve, 2000));

    console.log('\n📊 Verifying messages across topics:');

    const orderMessages = await orderConsumer.pollBatch(1, 5000);
    console.log(`  📦 Orders: ${orderMessages.length} messages`);

    const paymentMessages = await paymentConsumer.pollBatch(1, 5000);
    console.log(`  💳 Payments: ${paymentMessages.length} messages`);

    const inventoryMessages = await inventoryConsumer.pollBatch(1, 5000);
    console.log(`  📦 Inventory: ${inventoryMessages.length} messages`);

    console.log('\n🔄 Demonstrating transaction rollback...');

    await txProducer.begin();

    // Send some messages
    await txProducer.sendTransactional('orders-topic', {
      orderId: 'ORD-FAILED',
      status: 'created',
    });

    await txProducer.sendTransactional('payments-topic', {
      orderId: 'ORD-FAILED',
      status: 'failed',
    });

    // Simulate failure and abort
    await txProducer.abort();
    console.log('🚫 Transaction rolled back - no messages should be visible');

  } catch (error) {
    console.error('❌ Error:', error);
    try {
      await txProducer.abort();
    } catch (abortError) {
      console.error('❌ Failed to abort:', abortError);
    }
  } finally {
    await txProducer.close();
    await orderConsumer.close();
    await paymentConsumer.close();
    await inventoryConsumer.close();
  }
}

async function transactionWithTimeoutExample() {
  console.log('\n🚀 Starting Transaction with Timeout Example...\n');

  const config = new Config({
    bootstrapServers: 'localhost:9092',
    acks: 'all',
    transactionTimeout: 10000, // 10 seconds
  });

  const transactionalId = `timeout-tx-${Date.now()}`;
  const txProducer = new TransactionalProducer(transactionalId, config);

  try {
    await txProducer.begin();

    // Send a message
    await txProducer.sendTransactional('tx-demo-topic', {
      message: 'This transaction will timeout',
      timestamp: new Date().toISOString(),
    });

    console.log('⏳ Waiting longer than transaction timeout...');

    // Wait longer than transaction timeout
    await new Promise(resolve => setTimeout(resolve, 12000));

    // Try to commit (this should fail)
    try {
      await txProducer.commit();
      console.log('❓ Transaction committed (unexpected)');
    } catch (error) {
      console.log('🚫 Transaction timed out as expected:', error.message);
    }

  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await txProducer.close();
  }
}

// Run the examples
if (import.meta.main) {
  await basicTransactionExample();
  console.log('\n' + '='.repeat(60) + '\n');
  await batchTransactionExample();
  console.log('\n' + '='.repeat(60) + '\n');
  await crossTopicTransactionExample();
  console.log('\n' + '='.repeat(60) + '\n');
  // Note: Timeout example takes 12+ seconds
  // await transactionWithTimeoutExample();
}

export {
  basicTransactionExample,
  batchTransactionExample,
  crossTopicTransactionExample,
  transactionWithTimeoutExample,
};