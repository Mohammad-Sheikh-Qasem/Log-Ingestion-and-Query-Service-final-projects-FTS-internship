# Log Ingestion and Query Service

<p align="center">
  <strong>A high-throughput backend service for reliable log ingestion, querying, and aggregation.</strong>
</p>

<p align="center">
  Built with Node.js, TypeScript, Fastify, PostgreSQL, and Docker.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Node.js-22%2B-339933?style=for-the-badge&logo=node.js&logoColor=white" alt="Node.js">
  <img src="https://img.shields.io/badge/TypeScript-5.x-3178C6?style=for-the-badge&logo=typescript&logoColor=white" alt="TypeScript">
  <img src="https://img.shields.io/badge/Fastify-5.x-000000?style=for-the-badge&logo=fastify&logoColor=white" alt="Fastify">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
</p>

---

## Overview

**Log Ingestion and Query Service** is a backend system designed to ingest large volumes of application logs while maintaining reliable persistence, efficient querying, aggregation performance, and predictable behavior under high load.

The project focuses on practical backend engineering challenges such as:

* High-throughput log ingestion
* In-memory batch processing
* Bulk database inserts
* PostgreSQL connection pooling
* Efficient database indexing
* Cursor-based pagination
* Time-based aggregation
* Pre-aggregated rollups
* Query consistency
* Reliability under high load
* Dockerized deployment
* Performance benchmarking

The final benchmark achieved a throughput of **14,957 logs/second** with **0.0% errors**.

---

## Architecture

The system is organized around a REST API, an ingestion pipeline, query and aggregation services, and PostgreSQL.

```mermaid
flowchart LR

    Client["Client"]
    Benchmark["Benchmark / Load Generator"]

    API["Fastify API"]

    Routes["API Routes"]
    Validation["Request Validation"]

    Ingestion["Log Ingestion"]
    Batch["In-Memory Batching"]

    Query["Query Service"]
    Aggregate["Aggregation Service"]

    DB[("PostgreSQL")]

    Logs[("logs")]
    Rollups[("logs_rollup")]

    Client --> API
    Benchmark --> API

    API --> Routes
    Routes --> Validation

    Validation --> Ingestion
    Ingestion --> Batch
    Batch --> DB

    API --> Query
    API --> Aggregate

    Query --> DB
    Aggregate --> DB

    DB --> Logs
    DB --> Rollups
```

The architecture separates request handling, business logic, persistence, querying, and aggregation so that each part can be optimized independently.

---

## Log Ingestion Flow

The ingestion pipeline is designed to process large batches of logs efficiently while minimizing database round trips.

```mermaid
flowchart TD

    A["POST /logs"]
    B["Request Validation"]
    C{"Request Valid?"}
    D["Reject Request"]

    E["Add Logs to In-Memory Batch"]
    F{"Batch Ready?"}

    G["Continue Collecting"]
    H["Bulk Database Insert"]

    DB[("PostgreSQL")]
    R["Return Response"]

    A --> B
    B --> C

    C -->|No| D
    C -->|Yes| E

    E --> F
    F -->|No| G
    G --> E

    F -->|Yes| H
    H --> DB
    H --> R
```

---

## Batching

A major performance decision in the project is the use of **in-memory batching**.

A naive implementation would perform one database operation for every log:

```text
Log 1 ──► INSERT ──► PostgreSQL
Log 2 ──► INSERT ──► PostgreSQL
Log 3 ──► INSERT ──► PostgreSQL
Log 4 ──► INSERT ──► PostgreSQL
Log 5 ──► INSERT ──► PostgreSQL
...
```

At approximately 15,000 logs per second, this would generate a very large number of database operations and network round trips.

Instead, logs are grouped into batches:

```text
Log 1 ─┐
Log 2  │
Log 3  │
Log 4  ├────► In-Memory Batch
Log 5  │
Log 6  │
Log N ─┘
             │
             ▼
        Bulk INSERT
             │
             ▼
        PostgreSQL
```

This reduces:

* Database round trips
* SQL execution overhead
* Transaction overhead
* Connection pressure

and allows the service to process a much higher ingestion rate.

---

## Database Design

The application uses PostgreSQL as its primary persistence layer.

### Entity Relationship Diagram

```mermaid
erDiagram

    LOGS {
        BIGSERIAL id PK
        TIMESTAMPTZ timestamp
        VARCHAR service
        VARCHAR level
        TEXT message
        JSONB attributes
    }

    LOGS_ROLLUP {
        TIMESTAMPTZ bucket_start PK
        VARCHAR service PK
        VARCHAR level PK
        BIGINT count
    }

    LOGS ||--o{ LOGS_ROLLUP : "aggregated into"
```

> `logs_rollup` represents derived aggregation data. The relationship above is logical and does not imply a direct foreign-key relationship.

---

## `logs`

The `logs` table stores the raw application logs.

| Column       | Type          | Description                    |
| ------------ | ------------- | ------------------------------ |
| `id`         | `BIGSERIAL`   | Unique log identifier          |
| `timestamp`  | `TIMESTAMPTZ` | Log timestamp                  |
| `service`    | `VARCHAR`     | Service that generated the log |
| `level`      | `VARCHAR`     | Log severity                   |
| `message`    | `TEXT`        | Log message                    |
| `attributes` | `JSONB`       | Additional structured metadata |

---

## Rollup Data

The system maintains aggregated information to make aggregation queries more efficient.

```text
Raw Logs
   │
   │ aggregation
   ▼
┌─────────────────────────────┐
│       Rollup Data           │
├─────────────────────────────┤
│ bucket_start                │
│ service                     │
│ level                       │
│ count                       │
└─────────────────────────────┘
```

Instead of repeatedly processing a large number of raw logs, aggregation queries can operate on the smaller rollup dataset.

---

## Performance-Oriented Design

Several application and database decisions focus specifically on high-throughput workloads.

### Bulk Inserts

The application groups logs and writes them to PostgreSQL in batches rather than performing an individual insert for every log.

```text
Many Logs
    │
    ▼
Batch
    │
    ▼
Bulk Insert
    │
    ▼
PostgreSQL
```

---

### Connection Pooling

The application uses PostgreSQL connection pooling so database connections can be reused.

```text
                 Application
                      │
                      ▼
             ┌─────────────────┐
             │ Connection Pool │
             └────────┬────────┘
                      │
             ┌────────┼────────┐
             │        │        │
             ▼        ▼        ▼
          Conn 1   Conn 2   Conn N
             │        │        │
             └────────┼────────┘
                      ▼
                 PostgreSQL
```

This avoids creating a new database connection for every request.

---

### Database Indexing

Indexes are used to support common query patterns.

The indexing strategy focuses on fields commonly used for:

* Timestamp-based queries
* Service filtering
* Log-level filtering
* Combined filtering
* Stable ordering

Indexes reduce unnecessary PostgreSQL scans and improve query performance as the number of stored logs increases.

---

## Cursor-Based Pagination

The query API uses cursor-based pagination to provide stable traversal through large datasets.

```mermaid
flowchart LR

    Request["GET /logs"]
    Page1["Page 1"]
    Cursor1["Cursor"]
    Page2["Page 2"]
    Cursor2["Cursor"]
    Page3["Page 3"]

    Request --> Page1
    Page1 --> Cursor1
    Cursor1 --> Page2
    Page2 --> Cursor2
    Cursor2 --> Page3
```

This avoids relying exclusively on large `OFFSET` values, which can become increasingly expensive when navigating deep into a large dataset.

Cursor-based pagination also provides stable ordering between pages.

---

## Aggregation

The service supports time-based and grouped log aggregation.

```mermaid
flowchart LR

    Raw[("Raw Logs")]
    Aggregator["Aggregation Logic"]
    Rollup[("Rollup Data")]
    Query["Aggregate Query"]
    Result["API Response"]

    Raw --> Aggregator
    Aggregator --> Rollup
    Rollup --> Query
    Query --> Result
```

Using pre-aggregated data reduces the amount of raw data that needs to be processed for repeated aggregation requests.

The final benchmark achieved an aggregate P95 latency of approximately **100 ms**.

---

# API

## `GET /health`

Checks whether the application and database are available.

```http
GET /health
```

Example:

```bash
curl http://localhost:8080/health
```

---

## `POST /logs`

Accepts a batch of logs.

```http
POST /logs
Content-Type: application/json
```

Example:

```json
{
  "logs": [
    {
      "timestamp": "2026-08-22T10:00:00.000Z",
      "level": "info",
      "service": "api",
      "message": "User logged in"
    }
  ]
}
```

The request is validated before the logs enter the ingestion pipeline.

---

## `GET /logs`

Retrieves stored logs.

```http
GET /logs
```

Example:

```bash
curl http://localhost:8080/logs
```

The endpoint supports the query filters and pagination implemented by the service.

---

## `GET /logs/aggregate`

Provides aggregated log information.

```http
GET /logs/aggregate
```

Example:

```bash
curl http://localhost:8080/logs/aggregate
```

Aggregation can be used to summarize logs by supported time, service, and level dimensions.

---

# Benchmark Results

The final implementation was evaluated using the project benchmark suite.

```text
┌────────────────────────────────────────────┐
│              BENCHMARK RESULTS             │
├────────────────────────────────────────────┤
│                                            │
│ Correctness          15.0 / 15             │
│ Performance          41.3 / 50             │
│ Queries              13.2 / 15             │
│ Reliability          20.0 / 20             │
│                                            │
├────────────────────────────────────────────┤
│ Throughput           14,957 logs/sec       │
│ Error Rate           0.0%                  │
│ Ingestion P95        430 ms                │
│ Aggregate P95        100 ms                │
│ Query Consistency    4 / 4                 │
│ Reliability          4 / 4 scenarios       │
│                                            │
└────────────────────────────────────────────┘
```

---

## Correctness

```text
15 / 15 checks passed

[####################] 100%
```

All correctness checks passed.

---

## Performance

```text
41.3 / 50

[################----] 82.6%
```

The service achieved:

```text
Throughput:  14,957 logs/sec
Error Rate:  0.0%
P95:         430 ms
```

---

## Query Performance

```text
13.2 / 15

[##################--] 88.0%
```

Query benchmark results:

```text
Aggregate P95:       100 ms
Consistency Checks:  4 / 4
```

---

## Reliability

```text
20 / 20

[####################] 100%
```

All reliability scenarios passed:

```text
Scenario 1    PASS
Scenario 2    PASS
Scenario 3    PASS
Scenario 4    PASS

Result:       4 / 4
```

---

# Performance Summary

```text
                    SYSTEM PERFORMANCE

              ┌───────────────────────┐
              │  14,957 logs / sec    │
              └───────────┬───────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Batch Processing │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   Bulk Insert   │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   PostgreSQL    │
                 └─────────────────┘
```

The measured throughput demonstrates that the system can process approximately 15K logs per second under the benchmark environment.

---

# Error Rate

```text
Error Rate

0.0%

[####################] 100% successful
```

No errors were recorded during the final performance benchmark.

---

# Latency

```text
Ingestion P95

430 ms

[████████████████████████████████████████]
```

```text
Aggregate P95

100 ms

[██████████]
```

P95 represents the response time within which approximately 95% of requests completed.

---

# Testing

The benchmark evaluates the system across four major areas:

```mermaid
flowchart TD

    Benchmark["Benchmark"]

    Correctness["Correctness"]
    Performance["Performance"]
    Queries["Queries"]
    Reliability["Reliability"]

    Benchmark --> Correctness
    Benchmark --> Performance
    Benchmark --> Queries
    Benchmark --> Reliability
```

The final results were:

| Category    |    Result |
| ----------- | --------: |
| Correctness | 15.0 / 15 |
| Performance | 41.3 / 50 |
| Queries     | 13.2 / 15 |
| Reliability | 20.0 / 20 |

---

# Project Structure

```text
.
├── src/
│   ├── routes/
│   ├── services/
│   ├── repositories/
│   ├── schemas/
│   └── ...
│
├── Dockerfile
├── docker-compose.yml
├── benchmark-report.json
├── package.json
├── package-lock.json
├── tsconfig.json
└── README.md
```

---

# Running with Docker

## Requirements

* Docker Desktop
* Docker Compose

Check the installation:

```bash
docker --version
docker compose version
```

---

## Start the Application

```bash
docker compose up --build
```

Or run it in the background:

```bash
docker compose up -d --build
```

This starts the application and PostgreSQL database.

---

## Check Containers

```bash
docker compose ps
```

---

## View Application Logs

```bash
docker compose logs -f app
```

---

## Stop the Application

```bash
docker compose down
```

To remove the PostgreSQL volume as well:

```bash
docker compose down -v
```

> `docker compose down -v` removes the persisted database volume.

---

# Running Locally

Install dependencies:

```bash
npm install
```

Create the required environment configuration.

Example:

```env
PORT=8080
DATABASE_URL=postgres://postgres:postgres@localhost:5432/logs_db
```

Start the development server:

```bash
npm run dev
```

Build the project:

```bash
npm run build
```

---

# Technology Stack

```text
┌──────────────────────────────────────┐
│              Backend                 │
├──────────────────────────────────────┤
│ Node.js                              │
│ TypeScript                           │
│ Fastify                              │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│             Data Layer               │
├──────────────────────────────────────┤
│ PostgreSQL                           │
│ Connection Pooling                   │
│ Indexes                              │
│ Batch Processing                     │
│ Bulk Inserts                         │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│          Infrastructure              │
├──────────────────────────────────────┤
│ Docker                               │
│ Docker Compose                       │
└──────────────────────────────────────┘
```

---

# What This Project Demonstrates

This project focuses on practical backend engineering problems rather than simply implementing CRUD endpoints.

It demonstrates:

* Designing a log ingestion API
* Handling high-throughput workloads
* In-memory batch processing
* Bulk PostgreSQL inserts
* Connection pooling
* Database indexing
* Cursor-based pagination
* Time-based aggregation
* Pre-aggregated rollups
* Query consistency
* Reliability testing
* Performance benchmarking
* Docker-based deployment
* Designing for approximately 15K logs/sec workloads

---

# Key Technical Decisions

### Why Batching?

To reduce database round trips and improve throughput.

### Why PostgreSQL?

PostgreSQL provides reliable persistence, indexing, transactions, and powerful aggregation capabilities.

### Why Connection Pooling?

To reuse database connections and prevent the overhead of creating a connection for every request.

### Why Cursor Pagination?

To provide stable and predictable pagination without relying on increasingly expensive large offsets.

### Why Rollups?

To reduce the cost of repeated aggregation queries over large volumes of raw logs.

### Why Docker?

To provide a reproducible environment containing both the application and PostgreSQL database.

---

# Future Improvements

Potential future improvements include:

```text
Current System
      │
      ├── Horizontal API Scaling
      │
      ├── Message Queue
      │      ├── Kafka
      │      └── RabbitMQ
      │
      ├── Dedicated Ingestion Workers
      │
      ├── PostgreSQL Partitioning
      │
      ├── Read Replicas
      │
      ├── Distributed Caching
      │
      ├── Metrics and Tracing
      │
      ├── CI/CD Pipeline
      │
      └── Kubernetes Deployment
```

---

# Final Results

```text
┌────────────────────────────────────────────┐
│                                            │
│              FINAL RESULTS                 │
│                                            │
├────────────────────────────────────────────┤
│                                            │
│  Throughput          14,957 logs/sec       │
│  Error Rate          0.0%                  │
│  Ingestion P95       430 ms                │
│  Aggregate P95       100 ms                │
│                                            │
│  Correctness         15 / 15               │
│  Query Consistency   4 / 4                 │
│  Reliability         4 / 4                 │
│                                            │
└────────────────────────────────────────────┘
```

The final implementation achieved strong correctness and reliability while sustaining approximately **15,000 logs per second** under the benchmark workload.

---

# Repository

[Log Ingestion and Query Service](https://github.com/Mohammad-Sheikh-Qasem/Log-Ingestion-and-Query-Service-final-projects-FTS-internship)

---

# Author

   **Mohamad Sheikh Qasem**

   Computer Science Student

   Backend Development | Node.js | TypeScript | PostgreSQL
