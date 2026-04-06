# Global Weather Analytics Platform

> Production-grade real-time + batch weather data pipeline for 50 cities worldwide.  
> Built with Airflow, Kafka, dbt, MinIO, and Grafana. 100% free. Runs on Docker.

[![Python](https://img.shields.io/badge/Python-3.11-blue)]()
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-green)]()
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.5-orange)]()
[![dbt](https://img.shields.io/badge/dbt_Core-1.7-red)]()
[![Docker](https://img.shields.io/badge/Docker_Compose-14_services-informational)]()

## Demo

[ADD GRAFANA GIF HERE after recording with Loom/ScreenToGif]

## Architecture

![Architecture](docs/architecture.png)

Three layers working together:

**Batch (Airflow)** — Hourly DAG fetches 6 weather variables for 50 cities from Open-Meteo, stages raw JSON to MinIO with partitioned paths, loads to PostgreSQL with idempotent upserts, then runs 3 automated data quality checks.

**Streaming (Kafka)** — Producer polls every 5 minutes and emits to `weather.readings.raw`. A second topic `weather.alerts` fires when temperature exceeds 2 standard deviations from the 30-day baseline. Consumer writes to PostgreSQL in micro-batches of 100 with manual offset commit for at-least-once delivery.

**Analytics (dbt + Grafana)** — Five dbt mart models transform raw readings into business-ready tables including daily summaries, 7-day rolling averages, and statistical anomaly detection using z-scores. Four Grafana dashboards visualize the data live.

## Quick start
```bash
git clone https://github.com/ahmedmajid22/Full_weather-analytics-platform-Data-Project-.git
cd Full_weather-analytics-platform-Data-Project-
cp .env.example .env
docker compose up -d
```

Wait 2 minutes for services to initialize, then open:

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |

## Tech stack

| Layer | Tool | Why this choice |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | Industry standard, CeleryExecutor, retries, monitoring |
| Streaming | Apache Kafka 3.5 | Durable, partitioned, exactly-once with idempotent producer |
| Object storage | MinIO | S3-compatible, runs locally, partitioned Parquet — zero cost |
| Database | PostgreSQL 15 | Composite PK, indexes, window functions, FK constraints |
| Transformation | dbt Core 1.7 | Lineage graph, column-level tests, ephemeral intermediates |
| Dashboards | Grafana | Provisioned-as-code, geomap panel, threshold alerts |
| Monitoring | Prometheus + Pushgateway | Pipeline health metrics, data quality SLOs |

## Key engineering decisions

See [docs/adr/](docs/adr/) for full Architecture Decision Records.

- **Why Open-Meteo:** No API key, no rate limits, 80+ years of historical data. OpenWeatherMap's free tier caps at 1,000 calls/day — 50 cities × 24 hours = 1,200 calls needed. [(ADR 001)](docs/adr/001-why-open-meteo.md)
- **Why idempotent Kafka producer:** `acks=all` + `enable.idempotence=True` prevents message loss if the broker restarts mid-batch. [(ADR 003)](docs/adr/003-why-kafka-not-rabbitmq.md)
- **Why MinIO over AWS S3:** Fully S3-compatible, runs in Docker, zero cost, and the switch to real S3 in production requires changing only endpoint URL and credentials. [(ADR 002)](docs/adr/002-why-minio-not-s3.md)
- **Why IQR-based anomaly score alongside z-score:** Standard z-scores are sensitive to outliers. Robust IQR scoring handles seasonal spikes better — both are calculated in the `temperature_anomalies` mart.

## Data quality

Every pipeline run automatically checks:
- City coverage ≥ 45 of 50 cities loaded in the last 2 hours
- Null temperature rate = 0
- No readings outside the physically possible range of −90°C to 60°C

## Running tests
```bash
pip install pytest
pytest tests/ -v
```

## Project structure
```
├── airflow/dags/         — ETL and Kafka producer DAGs
├── kafka/                — Producer and consumer scripts
├── dbt/models/           — Staging and mart SQL models
├── grafana/dashboards/   — Dashboard JSON (auto-provisioned)
├── scripts/              — DB schema and city seed data
├── monitoring/           — Prometheus config
├── tests/                — pytest unit tests
└── docs/adr/             — Architecture Decision Records
```