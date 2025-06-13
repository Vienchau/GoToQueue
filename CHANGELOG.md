# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.1] - 2025-06-13

### Added
- **Worker ID tracking**: `Enqueue` now returns the worker ID that will process the task
- **Structured logging system**: New configurable logger with log levels (DEBUG, INFO, ERROR, SILENT)
- **Panic recovery**: Workers automatically recover from panics with detailed stack traces and metadata
- **OS signal handling**: Graceful shutdown with SIGINT/SIGTERM that drains queues before exit
- **Enhanced examples**: Restructured examples directory with better demonstrations

### Changed
- **Default log level**: Changed from DEBUG to SILENT for production-friendly defaults
- **Context handling**: Improved context management - automatically provides `context.Background()` when nil
- **Timeout implementation**: Removed separate `WithTimeout` option in favor of context-based timeouts

### Fixed
- **Queue draining**: Improved queue draining during shutdown to process remaining items
### Removed
- **WithTimeout option**: Removed in favor of context-based timeout handling

## [v0.1.0] - 2025-06-10

### Added
- Initial release of go-to-queue
- Key-based worker queue implementation
- Round-robin distribution strategy
- Context support with timeout and cancellation
- Item expiration functionality
- Metadata support for queue items
- Thread-safe concurrent processing
- Comprehensive test suite
- Example usage in `/example` directory

### Features
- Multiple distribution strategies (Key-based, Round-robin)
- Concurrent worker processing
- FIFO ordering within each worker
- Pool statistics and monitoring
- Graceful shutdown support
