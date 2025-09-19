# Changelog

All notable changes to this project will be documented in this file.

## [2.0.0] - 2024-12-XX

###  Major Improvements

#### Fixed TransactionalProducer API
- **Fixed**: `send_transactional(topic, value, key=None)` now works as expected
- **Added**: `send_batch_transactional()` for automatic batch transactions
- **Improved**: Better error messages for transaction failures

#### Enhanced Batch Operations
- **Fixed**: `send_batch()` now supports both tuple `(topic, value)` and dict formats
- **Added**: Mixed format support in single batch
- **Improved**: Better error reporting for individual message failures

#### Better Error Handling
- **Improved**: More descriptive error messages with context
- **Added**: Specific error types for different failure scenarios
- **Enhanced**: Consumer error handling for unknown topics and connection issues

###  Performance & Testing
- **Achieved**: 31,158 msg/s throughput in benchmarks
- **Achieved**: 3.98ms average end-to-end latency
- **Achieved**: 92% code coverage with 125/128 tests passing
- **Added**: Comprehensive performance benchmarks
- **Added**: Message size limit testing (validated 1MB Kafka limit)

### ️ Project Organization
- **Reorganized**: Test files into logical directory structure
  - `tests/unit/` - Unit tests (no external dependencies)
  - `tests/integration/` - Integration tests (requires Kafka)
  - `tests/performance/` - Performance benchmarks
- **Enhanced**: Examples with real-world scenarios
  - `examples/basic/` - Getting started examples
  - `examples/advanced/` - Microservices patterns (event sourcing, CQRS, sagas)
  - `examples/patterns/` - Streaming analytics patterns
  - `examples/production/` - Production deployment guides
- **Improved**: README to be more developer-friendly with practical examples

###  Configuration & Development
- **Updated**: `pyproject.toml` with comprehensive test configuration
- **Added**: Test markers for different test categories
- **Added**: Coverage reporting with HTML output
- **Added**: Development tooling configuration (ruff, mypy)

###  Cleanup
- **Removed**: Duplicate and temporary test files
- **Removed**: Old example.py (replaced by examples/ directory)
- **Cleaned**: Python cache files and test artifacts

## [1.x.x] - Previous Versions

### Initial Implementation
- Basic Producer and Consumer classes
- TransactionalProducer support
- Configuration management
- Convenience functions (send, consume)
- Built on confluent-kafka foundation