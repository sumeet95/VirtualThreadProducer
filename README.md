# VirtualThreadProducer

A project demonstrating the performance differences between **platform threads** and **virtual threads** when consuming Kafka events.

## Overview

This project consists of three modules that work together to simulate real-world message consumption scenarios:

- **KafkaProducerVirtualThreadTest**: Produces Kafka events that are consumed by the other modules
- **PlatformTest**: Simulates event consumption using traditional **platform threads** (OS-level threads)
- **VirtualThreadTest**: Simulates event consumption using **virtual threads** (lightweight, managed by the JVM)

## Purpose

The primary goal of this project is to benchmark and compare the performance characteristics of platform threads versus virtual threads when processing Kafka messages at scale. This is useful for understanding:

- Memory efficiency differences between thread models
- Throughput and latency variations
- Resource utilization (CPU, memory) under high concurrency
- Scalability potential with virtual threads

## Architecture

The project leverages **Docker** to containerize and orchestrate the components, ensuring consistent execution environments and easy deployment.

### Components

1. **Kafka Producer** - Generates test events to be consumed
2. **Platform Thread Consumer** - Traditional threading model consumer
3. **Virtual Thread Consumer** - Modern lightweight threading model consumer

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Java 21+ (for virtual thread support)

### Running the Project

```bash
docker-compose up
