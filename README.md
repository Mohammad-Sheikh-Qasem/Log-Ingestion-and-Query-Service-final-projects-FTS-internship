# Log Ingestion and Query Service

A high-performance backend service for **ingesting, storing, querying, and aggregating application logs at scale**.

The system is designed to handle high-throughput log ingestion while maintaining reliable query performance, data consistency, and fault tolerance. It uses **Node.js, TypeScript, PostgreSQL, Docker, and Fastify** to provide a production-oriented log management backend.

---

## 📊 Performance Benchmark

The final implementation was evaluated using the project benchmark suite.

| Category                  |              Result |
| ------------------------- | ------------------: |
| **Correctness**           |       **15.0 / 15** |
| **Performance**           |       **41.3 / 50** |
| **Throughput**            | **14,957 logs/sec** |
| **Error Rate**            |            **0.0%** |
| **P95 Latency**           |          **430 ms** |
| **Queries**               |       **13.2 / 15** |
| **Aggregate P95**         |          **100 ms** |
| **Query Consistency**     |           **4 / 4** |
| **Reliability**           |       **20.0 / 20** |
| **Reliability Scenarios** |           **4 / 4** |

### Overall Result

The service successfully achieved:

* **14,957 logs/second** ingestion throughput
* **0% ingestion errors**
* **430 ms P95 latency**
* **100 ms aggregate query P95**
* **15/15 correctness checks**
* **4/4 reliability scenarios**
* **4/4 query consistency checks**

These results demonstrate that the service can process a high volume of logs while maintaining correctness and reliable query behavior.

---

## 🎯 Project Overview

Modern applications and distributed systems continuously generate large amounts of logs.

A naive implementation might insert every incoming log into PostgreSQL individually:

```text
HTTP Request
     │
     ▼
Application
     │
     ▼
INSERT log
     │
     ▼
PostgreSQL
```

This approach creates a large number of database operations and network round trips.

For high-volume workloads, such as **15,000+ logs per second**, this can quickly become a bottleneck.

This project addresses that problem using an optimized ingestion architecture based on:

* In-memory batching
* Bulk database inserts
* PostgreSQL indexing
* Efficient query filtering
* Cursor-based pagination
* Pre-aggregated rollups
* Connection pooling
* Dockerized deployment

---

## 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │    Log Producers    │
                    │  / Benchmark Tool   │
                    └──────────┬──────────┘
                               │
                               │ HTTP
                               ▼
                    ┌─────────────────────┐
                    │     Fastify API     │
                    │                     │
                    │  POST /logs         │
                    │  GET  /logs         │
                    │  GET  /logs/aggregate│
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   Log Service       │
                    │                     │
                    │ Validation          │
                    │ Batching             │
                    │ Query Logic          │
                    │ Aggregation          │
                    └──────────┬──────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
      ┌──────────────────┐          ┌──────────────────┐
      │ PostgreSQL       │          │ Rollup/Aggregate │
      │                  │          │ Data             │
      │ logs             │          │                  │
      │ indexes          │          │ bucket/service/  │
      │                  │          │ level/count      │
      └──────────────────┘          └──────────────────┘
```

---

# 🚀 Key Technical Decisions

## 1. In-Memory Batching

One of the most important performance decisions is **batching incoming logs before inserting them into PostgreSQL**.

Instead of:

```text
Log 1 → INSERT
Log 2 → INSERT
Log 3 → INSERT
Log 4 → INSERT
...
```

the service groups logs together:

```text
Log 1 ─┐
Log 2  │
Log 3  ├──► Batch ──► Bulk INSERT ──► PostgreSQL
Log 4  │
Log 5 ─┘
```

This significantly reduces:

* Database round trips
* SQL statement overhead
* Connection usage
* Transaction overhead

It is particularly important for workloads approaching **15,000 logs/sec**.

---

## 2. PostgreSQL Connection Pooling

The application uses a PostgreSQL connection pool instead of opening a new database connection for every request.

```text
                Application
                     │
                     ▼
              Connection Pool
          ┌──────┬──────┬──────┐
          │ Conn │ Conn │ Conn │
          └──┬───┴──┬───┴──┬───┘
             │      │      │
             └──────┴──────┘
                    │
                    ▼
                PostgreSQL
```

Connection pooling improves scalability by allowing database connections to be reused efficiently.

---

## 3. Bulk Inserts

Logs are inserted in batches instead of executing an individual SQL operation for every log.

Conceptually:

```sql
INSERT INTO logs (...)
VALUES
  (...),
  (...),
  (...),
  (...);
```

This reduces the number of database operations required to process a large workload.

---

## 4. Efficient Querying

The service supports querying logs using filters such as:

* Service
* Log level
* Time range
* Other supported fields

Database indexes are used to avoid unnecessary full-table scans.

The goal is to keep query performance predictable even as the number of stored logs grows.

---

## 5. Cursor-Based Pagination

The API uses cursor-based pagination rather than relying exclusively on large `OFFSET` values.

Conceptually:

```text
Page 1
  │
  ▼
last record cursor
  │
  ▼
Page 2
  │
  ▼
next cursor
  │
  ▼
Page 3
```

This provides more stable performance when navigating through large datasets.

It also helps maintain deterministic ordering across pages.

---

## 6. Aggregation and Rollups

The system supports aggregate queries that summarize logs by dimensions such as:

* Time bucket
* Service
* Log level

Instead of repeatedly scanning the entire raw logs table for every aggregation request, aggregate information can be maintained in rollup data.

Example:

```text
Raw Logs

10:01 service-A ERROR
10:01 service-A ERROR
10:01 service-A INFO
10:01 service-B ERROR
        │
        ▼
     Rollup
        │
        ▼
┌────────────┬─────────┬───────┬───────┐
│ Bucket     │ Service │ Level │ Count │
├────────────┼─────────┼───────┼───────┤
│ 10:01      │ A       │ ERROR │ 2     │
│ 10:01      │ A       │ INFO  │ 1     │
│ 10:01      │ B       │ ERROR │ 1     │
└────────────┴─────────┴───────┴───────┘
```

This allows aggregate queries to remain fast.

The benchmark achieved an **aggregate P95 latency of approximately 100 ms**.

---

# ✨ Features

## Log Ingestion

* HTTP-based log ingestion
* Batch processing
* Bulk PostgreSQL inserts
* Request validation
* High-throughput processing
* Zero errors in the final benchmark

## Log Querying

* Retrieve stored logs
* Filtering
* Time-based queries
* Stable ordering
* Cursor pagination
* Consistent results

## Aggregation

* Time-based aggregation
* Service-level aggregation
* Log-level aggregation
* Efficient rollup queries

## Reliability

The implementation was tested against multiple reliability scenarios.

Final result:

**4 / 4 reliability scenarios passed.**

---

# 🛠️ Tech Stack

| Technology                  | Purpose                                |
| --------------------------- | -------------------------------------- |
| **Node.js**                 | Runtime                                |
| **TypeScript**              | Application development                |
| **Fastify**                 | HTTP API framework                     |
| **PostgreSQL**              | Persistent data storage                |
| **Docker**                  | Containerization                       |
| **Docker Compose**          | Multi-container orchestration          |
| **Zod / Schema Validation** | Input validation                       |
| **pg**                      | PostgreSQL client / connection pooling |

---

# 📁 Project Structure

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
├── package.json
├── package-lock.json
├── tsconfig.json
├── benchmark-report.json
└── README.md
```

The repository contains the application source code under `src`, together with Docker configuration, TypeScript configuration, dependency manifests, and the benchmark report.

---

# ⚙️ Getting Started

## Prerequisites

Make sure the following are installed:

* Node.js
* npm
* Docker
* Docker Compose

You can verify the installations with:

```bash
node --version
npm --version
docker --version
docker compose version
```

---

# 📥 Installation

Clone the repository:

```bash
git clone https://github.com/Mohammad-Sheikh-Qasem/Log-Ingestion-and-Query-Service-final-projects-FTS-internship.git
```

Enter the project directory:

```bash
cd Log-Ingestion-and-Query-Service-final-projects-FTS-internship
```

Install dependencies:

```bash
npm install
```

---

# 🐳 Running with Docker

Build and start the application:

```bash
docker compose up --build
```

To run in detached mode:

```bash
docker compose up -d --build
```

Check running containers:

```bash
docker compose ps
```

View application logs:

```bash
docker compose logs -f app
```

Stop the application:

```bash
docker compose down
```

---

# 🔌 API

The service exposes an HTTP API for ingesting and querying logs.

## Health Check

```http
GET /health
```

Example:

```bash
curl http://localhost:8080/health
```

---

# 📥 Ingest Logs

```http
POST /logs
```

Example request:

```json
{
  "logs": [
    {
      "timestamp": "2026-08-22T10:00:00Z",
      "service": "api",
      "level": "ERROR",
      "message": "Database connection failed"
    }
  ]
}
```

The endpoint validates the incoming payload and sends logs through the batching and persistence pipeline.

---

# 🔎 Query Logs

```http
GET /logs
```

The query endpoint supports the available filtering and pagination parameters implemented by the service.

Example:

```bash
curl "http://localhost:8080/logs"
```

Filters can be combined to narrow the result set.

---

# 📊 Aggregate Logs

```http
GET /logs/aggregate
```

The aggregation endpoint returns summarized log information based on the supported aggregation dimensions.

Example:

```bash
curl "http://localhost:8080/logs/aggregate"
```

---

# 🧪 Testing

The project was tested using the benchmark suite covering four major areas:

### Correctness

All correctness checks passed:

```text
15 / 15
```

### Performance

```text
Throughput: 14,957 logs/sec
Errors:     0.0%
P95:        430 ms
```

### Queries

```text
Query Score:        13.2 / 15
Aggregate P95:      100 ms
Consistency Checks: 4 / 4
```

### Reliability

```text
Reliability Score: 20 / 20
Scenarios Passed:  4 / 4
```

---

# 📈 Benchmark Summary

```text
                    FINAL BENCHMARK

Correctness  ████████████████████  15.0 / 15
Performance  █████████████████░░░  41.3 / 50
Queries      ██████████████████░░  13.2 / 15
Reliability  ████████████████████  20.0 / 20
```

### Throughput

```text
14,957 logs / second
```

### Error Rate

```text
0.0%
```

### P95 Ingestion Latency

```text
430 ms
```

### Aggregate P95

```text
100 ms
```

---

# 🔐 Reliability

Reliability was a major design goal of the project.

The final benchmark achieved:

**4/4 reliability scenarios passed.**

The service was designed to maintain correct behavior under different operational conditions while preventing data corruption and maintaining consistent query results.

---

# ⚡ Performance Engineering

The implementation focuses on avoiding common high-throughput bottlenecks.

### Instead of

```text
15,000 logs/sec
       │
       ▼
15,000 individual INSERT operations
       │
       ▼
Database bottleneck
```

### The system uses

```text
15,000 logs/sec
       │
       ▼
In-Memory Batch
       │
       ▼
Bulk INSERT
       │
       ▼
PostgreSQL
```

This architecture reduces database interaction overhead and allows the application to process significantly more logs per second.

---

# 🧠 Design Principles

The project follows several important backend engineering principles:

### Separation of Concerns

The application separates:

```text
Routes
  ↓
Services
  ↓
Repositories
  ↓
Database
```

This keeps HTTP handling, business logic, and database access independent.

### Validate Early

Invalid requests are rejected before reaching the persistence layer.

### Minimize Database Round Trips

Batching and bulk operations reduce unnecessary database communication.

### Index for the Access Pattern

Indexes are designed around the fields used by the query workload rather than indexing every column blindly.

### Stable Pagination

Cursor-based pagination provides predictable behavior for large datasets.

### Optimize the Hot Path

The ingestion path is optimized because it is the most frequently executed operation in the system.

---

# 📦 Docker Architecture

The application is containerized together with PostgreSQL.

```text
┌──────────────────────────────────────┐
│            Docker Compose            │
│                                      │
│   ┌──────────────┐   ┌────────────┐  │
│   │     App      │──►│ PostgreSQL │  │
│   │   Fastify    │   │    DB      │  │
│   └──────────────┘   └────────────┘  │
│          │                           │
│          │ Port 8080                 │
└──────────┼───────────────────────────┘
           │
           ▼
        Client
```

This makes the project reproducible across development and testing environments.

---

# 🧹 Stopping the Environment

Stop the containers:

```bash
docker compose down
```

To remove containers and associated volumes:

```bash
docker compose down -v
```

> Use `-v` carefully because it removes the PostgreSQL volume and therefore deletes persisted database data.

---

# 📌 Performance Considerations

The benchmark demonstrates that the current implementation is capable of approximately **15K logs/sec** under the tested environment.

Actual throughput depends on:

* CPU allocation
* Available RAM
* Docker resource limits
* PostgreSQL configuration
* Disk performance
* Network latency
* Batch size
* Workload characteristics

Therefore, benchmark results should be interpreted as measurements for the tested environment rather than an absolute hardware-independent limit.

---

# 🚀 Future Improvements

Potential future improvements include:

* Horizontal application scaling
* Message queues such as Kafka or RabbitMQ
* Distributed ingestion workers
* PostgreSQL partitioning for very large datasets
* Read replicas
* Advanced caching
* More sophisticated aggregation strategies
* Observability with Prometheus and Grafana
* Automated CI/CD pipelines
* Kubernetes deployment
* Rate limiting and authentication
* Full-text search optimization

---

# 📚 What This Project Demonstrates

This project demonstrates practical experience with:

* Backend API development
* TypeScript
* Node.js
* Fastify
* PostgreSQL
* Database indexing
* Connection pooling
* Batch processing
* Bulk inserts
* Cursor pagination
* Data aggregation
* Performance optimization
* Docker
* Load testing
* Reliability testing
* System design

The most important engineering lesson is that **high-throughput systems require optimization of the entire data path**, not just the HTTP layer.

---

# 👨‍💻 Author

**Mohamad Sheikh Qasem**

Computer Science Student
Backend Development | Java | TypeScript | PostgreSQL

GitHub:

https://github.com/Mohammad-Sheikh-Qasem

---

# 📄 License

This project was developed as part of an internship project and technical evaluation.

---

## ⭐ Project Highlights

```text
┌─────────────────────────────────────────┐
│       LOG INGESTION & QUERY SERVICE     │
├─────────────────────────────────────────┤
│                                         │
│  🚀 14,957 logs/sec                     │
│  ⚡ 430 ms P95 latency                  │
│  📊 100 ms aggregate P95                │
│  ✅ 15/15 correctness                   │
│  ✅ 0.0% errors                         │
│  ✅ 4/4 reliability scenarios           │
│  🐘 PostgreSQL                          │
│  🟦 TypeScript + Node.js                │
│  🚀 Fastify                             │
│  🐳 Docker                              │
│                                         │
└─────────────────────────────────────────┘
```
