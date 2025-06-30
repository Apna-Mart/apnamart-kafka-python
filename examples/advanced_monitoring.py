"""Advanced monitoring and observability example."""

import time

from apnamart_kafka import (
    BasicMonitoringHandler,
    KafkaConfig,
    KafkaProducer,
    MetricsCollector,
)
from apnamart_kafka.common.monitoring import MonitoringCollectorHandler


def monitoring_example():
    """Example showcasing monitoring and metrics collection."""
    print("=== Advanced Monitoring Example ===")

    # Create a metrics collector
    metrics_collector = MetricsCollector()

    # Create monitoring handlers
    basic_handler = BasicMonitoringHandler(log_level="INFO")
    metrics_handler = MonitoringCollectorHandler(metrics_collector)

    # Configuration with monitoring enabled
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="monitored-app",
        enable_metrics=True,
        metrics_registry="prometheus",
    )

    with KafkaProducer(config=config) as producer:
        # Add monitoring handlers
        producer.add_monitoring_handler(basic_handler)
        producer.add_monitoring_handler(metrics_handler)

        print("🔍 Monitoring handlers added")

        try:
            # Send some messages to generate metrics
            for i in range(10):
                producer.send(
                    topic="user-events",
                    value={
                        "user_id": f"user-{i}",
                        "action": "page_view",
                        "page": f"/product/{i}",
                        "timestamp": time.time(),
                    },
                    key=f"user-{i}",
                )

                # Send to different topic
                producer.send(
                    topic="system-metrics",
                    value={
                        "metric": "cpu_usage",
                        "value": 45.6 + i,
                        "timestamp": time.time(),
                    },
                )

            producer.flush()
            print("✅ All messages sent successfully")

            # Get metrics
            metrics = metrics_collector.get_metrics()
            print("\n📊 Collected Metrics:")
            print(f"Total messages sent: {metrics['global']['messages_sent']}")
            print(f"Total bytes sent: {metrics['global']['bytes_sent']}")
            print(f"Messages failed: {metrics['global']['messages_failed']}")

            print("\n📈 Topic-specific metrics:")
            for topic, topic_metrics in metrics["topics"].items():
                print(
                    f"  {topic}: {topic_metrics['messages_sent']} messages, {topic_metrics['bytes_sent']} bytes"
                )

            # Get producer metrics
            producer_metrics = producer.get_metrics()
            print(f"\n🔧 Producer metrics: {producer_metrics}")

        except Exception as e:
            print(f"❌ Error: {e}")


def plugin_system_example():
    """Example showcasing the plugin system."""
    print("\n=== Plugin System Example ===")

    # Get the global plugin manager
    from apnamart_kafka.common.plugins import plugin_manager

    # Register a custom monitoring plugin
    class CustomMonitoringHandler(BasicMonitoringHandler):
        def on_message_sent(self, topic, key, value, metadata):
            print(f"🚀 Custom handler: Message sent to {topic}")

    # Register the plugin
    plugin_manager.register_plugin(
        category="monitoring",
        name="custom",
        plugin_class=CustomMonitoringHandler,
        log_level="DEBUG",
    )

    # List available plugins
    plugins = plugin_manager.list_plugins()
    print(f"📦 Available plugins: {plugins}")

    # Get and use the plugin
    custom_handler = plugin_manager.get_plugin("monitoring", "custom")

    with KafkaProducer() as producer:
        if custom_handler:
            producer.add_monitoring_handler(custom_handler)
            print("✅ Custom monitoring handler added")

            # Send a test message
            producer.send("test-topic", {"message": "Plugin system test"})
            producer.flush()


def extensible_config_example():
    """Example showing extensible configuration for future features."""
    print("\n=== Extensible Configuration Example ===")

    # Configuration with future features
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="enterprise-app",
        # Security settings (Phase 2)
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="myuser",
        sasl_password="mypassword",
        # Monitoring settings (Phase 2)
        enable_metrics=True,
        metrics_registry="prometheus",
        # Schema registry settings (Phase 3)
        schema_registry_url="http://localhost:8081",
        schema_registry_auth="basic:user:pass",
        # Cloud platform settings (Phase 3)
        cloud_provider="aws",
        cloud_region="us-west-2",
    )

    print("🔧 Configuration created with future features:")
    print(f"Security config: {config.get_security_config()}")
    print(f"Monitoring config: {config.get_monitoring_config()}")
    print(f"Schema registry config: {config.get_schema_registry_config()}")
    print(f"Cloud config: {config.get_cloud_config()}")


def health_check_example():
    """Example showing enhanced health check capabilities."""
    print("\n=== Enhanced Health Check Example ===")

    producer = KafkaProducer()

    try:
        # Perform health check
        health = producer.health_check()
        print(f"🏥 Health Status: {health['status']}")
        print(f"🔌 Connection: {health['connection']}")
        print(f"📡 Bootstrap Servers: {health['bootstrap_servers']}")
        print(f"⚙️  Configuration: {health.get('config', {})}")

        if health["status"] == "unhealthy":
            print(f"❌ Error: {health.get('error', 'Unknown error')}")
        else:
            print("✅ System is healthy!")

    except Exception as e:
        print(f"❌ Health check error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    print("🚀 Advanced Kafka Producer Features")
    print("=" * 50)

    # Run examples
    monitoring_example()
    plugin_system_example()
    extensible_config_example()
    health_check_example()

    print("\n✨ Advanced examples completed!")
    print("\n💡 Architecture Benefits:")
    print("- ✅ Extensible monitoring and observability")
    print("- ✅ Plugin system for custom functionality")
    print("- ✅ Configuration ready for enterprise features")
    print("- ✅ Enhanced health checking and metrics")
    print("- ✅ Foundation for future TODO.md phases")
