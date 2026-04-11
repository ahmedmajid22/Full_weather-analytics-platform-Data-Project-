# Global Weather Analytics Platform

> End-to-end data engineering platform: real-time and batch weather data for 50 cities worldwide.
> Built with Apache Airflow, Apache Kafka, dbt, MinIO, PostgreSQL, and Grafana.
> Runs entirely on Docker Compose — zero cloud spend.

![Python 3.11](1.png)
![Apache Airflow](2.png)
![Apache Kafka](3.png)
![dbt](4.png)
![PostgreSQL](5.png)
![Grafana](6.png)

---

## Architecture

```mermaid
graph TD
    A[Open-Meteo API (free, no key)] --> B{BATCH LAYER (Airflow)}
    B -- weather_etl DAG runs @hourly --> C[fetch_and_stage]
    C --> D[MinIO (raw JSON, partitioned)]
    C --> E[load_to_postgres]
    E --> F[PostgreSQL (weather schema)]
    E --> G[validate_data]
    G --> H[DQ checks + Prometheus metrics]
    B --> I{STREAMING LAYER (Kafka)}
    I -- Producer polls every 5 min --> J[weather.readings.raw]
    I -- Producer polls every 5 min --> K[weather.alerts (z > 2σ)]
    J --> L[Consumer batch-inserts --> streaming_readings (100/commit)]
    K --> L
    I --> M{ANALYTICS LAYER (dbt + Grafana)}
    M -- 5 mart models --> N[daily_summary, weekly_trends, city_comparisons, temperature_anomalies, extreme_weather]
    N --> O[4 Grafana dashboards]
    O --> P[Global map · City deep dive · Anomaly tracker · Pipeline health]
```

---

## Quick start

```bash
git clone https://github.com/YOUR_USERNAME/weather-analytics-platform
cd weather-analytics-platform
cp .env.example .env
docker compose up -d
```

Wait ~90 seconds for all services to initialize, then:

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |
| Prometheus | http://localhost:9090 | — |

Trigger the first DAG run manually in Airflow to populate the database immediately, or wait for the `@hourly` schedule to fire automatically.

Run dbt transformations (after at least one DAG run):

```bash
cd dbt
dbt run
dbt test
```

---

## What this platform does

**50 cities monitored** across 6 continents, including deliberately contrasting climates: Reykjavik, Anchorage, Ulaanbaatar (extreme cold) alongside Dubai, Bangkok, Lagos (extreme heat).

**Batch pipeline** (Airflow): fetches 6 weather variables per city every hour → stages raw JSON to MinIO in Hive-style partitions (`city=/year=/month=/day=/hour=`) → cleans and loads to PostgreSQL → runs 3 data quality checks automatically → pushes metrics to Prometheus.

**Streaming pipeline** (Kafka): producer polls Open-Meteo every 5 minutes and emits to two topics — `weather.readings.raw` (all readings) and `weather.alerts` (fired when temperature deviates more than 2 standard deviations from the 30-day city baseline). Consumer writes in micro-batches of 100 with manual commit offset for at-least-once delivery.

**Analytics layer** (dbt): 5 mart models including statistical anomaly detection using 30-day rolling z-scores and IQR-based robust z-scores. Schema tests run on every model.

**Dashboards** (Grafana): provisioned as code — cloning the repo gives you all 4 dashboards automatically. Includes a geomap colored by average temperature, an interactive city dropdown for the deep dive panel, and an anomaly tracker with z-score threshold lines.

---

## Tech stack and why

| Layer | Tool | Decision rationale |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | Industry standard; CeleryExecutor mirrors production setups |
| Streaming | Apache Kafka 3.5 | Durable, partitioned, replayable; RabbitMQ deletes on consume |
| Object storage | MinIO | S3-compatible API; zero cost; swappable to AWS S3 without code changes |
| Warehouse | PostgreSQL 15 | Composite primary key; partial indexes; window functions for analytics |
| Transformation | dbt Core 1.7 | Lineage graph; column-level tests; ephemeral intermediates |
| Dashboards | Grafana | Provisioned-as-code JSON; no manual setup after clone |
| Monitoring | Prometheus + Pushgateway | Pipeline health metrics; data quality SLOs |
| Data source | Open-Meteo API | Free, no API key, no rate limits, 80+ years of historical data |

Full decisions with alternatives considered: [`docs/adr/`](docs/adr/)

---

## Key engineering decisions

**Idempotent loading:** Every `INSERT` uses `ON CONFLICT DO UPDATE`, so the DAG is safe to rerun without creating duplicate rows — essential for Airflow\'s retry mechanism.

**Exactly-once Kafka delivery:** Producer is configured with `acks=all`, `enable.idempotence=True`, and `compression.type=snappy`. This combination prevents message loss if a broker restarts mid-batch and avoids silent duplicates.

**Manual consumer commit:** `enable.auto.commit=False` with `consumer.commit(asynchronous=False)` means if the consumer crashes mid-batch it will reprocess from the last committed offset — no silent data loss.

**Two anomaly scoring methods:** The `temperature_anomalies` mart computes both standard z-score and IQR-based robust z-score. Standard z-score is sensitive to outliers; a single extreme reading inflates the standard deviation and can mask subsequent anomalies. IQR-based scoring is more robust for weather data which has seasonal spikes.

**Minimum sample guard:** The anomaly mart requires at least 24 readings per city (one full day) before computing z-scores. This prevents misleading anomaly flags from tiny samples on pipeline startup.

---

## Data quality

Three automated checks run after every hourly load:

| Check | Threshold | Action on failure |
|---|---|---|
| City coverage | ≥ 45 / 50 cities loaded per hour | Logged; visible in Pipeline Health dashboard |
| Null temperatures | 0 null rows in last 2 hours | Logged |
| Extreme values | No temperature outside −90°C to 60°C | Logged |

---

## Project structure

```
weather-analytics-platform/
├── airflow/dags/
│   ├── weather_etl_dag.py        # Main hourly ETL pipeline
│   └── kafka_producer_dag.py     # Streaming freshness monitor
├── kafka/
│   ├── producer.py               # Polls API every 5 min, emits to Kafka
│   └── consumer.py               # Batch-inserts from Kafka to PostgreSQL
├── dbt/models/
│   ├── staging/                  # Clean and validate raw data
│   └── marts/                    # Aggregated analytics tables
├── grafana/
│   ├── dashboards/               # 4 pre-built dashboards as JSON
│   └── datasources/              # Auto-provisioned PostgreSQL + Prometheus
├── scripts/
│   ├── init_db.sql               # Schema creation with indexes
│   └── seed_cities.sql           # 50 cities across 6 continents
├── docs/adr/                     # Architecture Decision Records
├── monitoring/prometheus.yml
├── tests/
│   ├── test_etl.py               # Unit tests for ETL functions
│   └── test_dbt_models.py        # Integration tests against live DB
└── docker-compose.yml            # All 14 services
```

---

## Running tests

Unit tests (no DB required):

```bash
pip install pytest requests psycopg2-binary
pytest tests/test_etl.py -v
```

Integration tests (requires running DB + dbt run):

```bash
POSTGRES_HOST=localhost POSTGRES_PORT=5433 pytest tests/test_dbt_models.py -v
```

---

## Services started by Docker Compose

14 containers total:

| Container | Purpose |
|---|---|
| `postgres` | Primary data warehouse |
| `redis` | Airflow Celery broker |
| `airflow-webserver` | Airflow UI |
| `airflow-scheduler` | DAG scheduling |
| `airflow-worker` | Task execution |
| `airflow-init` | One-time DB migration |
| `zookeeper` | Kafka coordination |
| `kafka` | Message broker |
| `kafka-ui` | Kafka topic browser |
| `kafka-producer` | Weather streaming producer |
| `kafka-consumer` | Kafka → PostgreSQL sink |
| `minio` | S3-compatible object store |
| `minio-init` | Bucket creation |
| `prometheus` | Metrics collection |
| `pushgateway` | Airflow → Prometheus bridge |
| `grafana` | Dashboards |