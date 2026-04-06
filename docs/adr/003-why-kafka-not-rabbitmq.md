# ADR 003: Why Kafka instead of RabbitMQ

## Status
Accepted

## Context
The platform requires a streaming layer to process near real-time weather data.

Requirements:
- Handle continuous data streams (every 5 minutes)
- Support multiple consumers (analytics, alerts, storage)
- Retain data for replay and debugging
- Scale horizontally with partitions
- Provide durability guarantees

Options considered:
- RabbitMQ (message broker)
- Apache Kafka (distributed event streaming platform)

## Decision
We use Apache Kafka as the streaming platform.

## Reasons

### 1. Data Retention & Replay
- Kafka stores messages for a configurable retention period
- Consumers can replay historical data at any time
- RabbitMQ deletes messages after consumption (by default)

### 2. Scalability
- Kafka supports partitioned topics
- Allows horizontal scaling of producers and consumers
- Handles high-throughput workloads efficiently

### 3. Event Streaming Model
- Kafka is designed for event streaming, not just messaging
- Multiple consumers can independently read the same data
- Enables building data pipelines and analytics systems

### 4. Durability & Reliability
- Kafka supports replication across brokers
- With `acks=all` and idempotent producers, it provides strong delivery guarantees
- Better suited for critical data pipelines

## Consequences

### Positive
- Scalable and durable streaming system
- Supports multiple downstream consumers
- Enables replay and debugging of data
- Aligns with modern data engineering architectures

### Negative
- More complex setup compared to RabbitMQ
- Requires understanding of partitions, offsets, and brokers
- Higher resource usage

## Conclusion
Kafka is the better choice for a data engineering platform that requires scalable, durable, and replayable event streams.