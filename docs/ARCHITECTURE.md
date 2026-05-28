# Architecture Documentation
## Lambda Architecture Big Data Platform — Real-Time Satellite Tracking

---

## Table of Contents

1. [Project Purpose](#1-project-purpose)
2. [Architecture Overview](#2-architecture-overview)
3. [ASCII Architecture Diagram](#3-ascii-architecture-diagram)
4. [Layer-by-Layer Explanation](#4-layer-by-layer-explanation)
   - 4.1 [Ingestion Layer (Data Sources)](#41-ingestion-layer-data-sources)
   - 4.2 [Message Bus (Kafka)](#42-message-bus-kafka)
   - 4.3 [Speed Layer](#43-speed-layer)
   - 4.4 [Batch Layer](#44-batch-layer)
   - 4.5 [Serving Layer](#45-serving-layer)
5. [Data Flow Walkthrough — One Position Message End to End](#5-data-flow-walkthrough--one-position-message-end-to-end)
6. [Key Design Decisions](#6-key-design-decisions)
7. [Technology Stack Table](#7-technology-stack-table)
8. [Limitations and Known Constraints](#8-limitations-and-known-constraints)

---

## 1. Project Purpose

This platform demonstrates the **Lambda Architecture** pattern applied to a real-world, continuously evolving dataset: satellite telemetry. Three real satellites are tracked — the International Space Station (ISS), Hubble Space Telescope, and NOAA-20 — using publicly available APIs.

The core demonstration goal is to show how the same logical question ("where is this satellite, and is anything anomalous about it?") can be answered with two fundamentally different computational strategies that run simultaneously and complement each other:

- The **speed layer** answers in seconds, using approximate, windowed computations.
- The **batch layer** answers with complete historical accuracy after processing all accumulated data overnight.
- The **serving layer** merges both answers so that end-users and downstream systems always receive the best available result.

This architecture is a reference implementation for Big Data systems that must balance freshness against correctness — a trade-off that is fundamental in domains such as financial risk, IoT monitoring, logistics tracking, and space operations.

---

## 2. Architecture Overview

Lambda Architecture divides data processing into three independent layers, each with a distinct latency profile and correctness guarantee:

```
LAYER               LATENCY         CORRECTNESS         TECHNOLOGY
---------------------------------------------------------------------------
Ingestion           Continuous      Raw (unvalidated)   Python Producers
Speed               Seconds         Approximate         Kafka + Spark Streaming
Batch               Hours           Complete/Accurate   HDFS + Spark + MapReduce
Serving             Milliseconds    Merged              FastAPI + Redis + Grafana
```

The ingestion layer feeds **both** the speed and batch layers simultaneously via Kafka. Neither layer waits for the other. The serving layer is the only component that is aware of both layers; it merges their outputs at query time.

---

## 3. ASCII Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                        INGESTION LAYER (Python Producers)                       ║
║                                                                                  ║
║  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────┐ ║
║  │  ISSProducer    │  │  N2YOProducer    │  │  DONKIProducer   │  │  TLE     │ ║
║  │  (every 5s)     │  │  (every 15s)     │  │  (every 300s)    │  │  Producer│ ║
║  │  Open-Notify    │  │  N2YO API        │  │  NASA DONKI      │  │  (3600s) │ ║
║  │  ISS position   │  │  ISS+Hubble+     │  │  CME,FLR,HSS    │  │  Orbital │ ║
║  │                 │  │  NOAA-20         │  │                  │  │  Elements│ ║
║  └────────┬────────┘  └────────┬─────────┘  └────────┬─────────┘  └────┬─────┘ ║
╚═══════════╪════════════════════╪════════════════════════╪════════════════╪══════╝
            │                   │                        │                │
            ▼                   ▼                        ▼                ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                         MESSAGE BUS (Apache Kafka + Zookeeper)                  ║
║                                                                                  ║
║  ┌─────────────────────┐  ┌─────────────────────┐  ┌────────────────────────┐  ║
║  │  sat.position.raw   │  │  sat.events.raw      │  │  sat.tle.raw           │  ║
║  │  (ISS + N2YO)       │  │  (NASA space weather)│  │  (orbital elements)    │  ║
║  └──────────┬──────────┘  └──────────┬───────────┘  └────────────────────────┘  ║
║             │                        │                                           ║
║  ┌──────────┴──────────┐  ┌──────────┴───────────┐  ┌────────────────────────┐  ║
║  │ sat.position.enriched│  │  sat.alerts          │  │  sat.batch.trigger     │  ║
║  │ (Spark output)      │  │  (anomaly output)    │  │  (Airflow signals)     │  ║
║  └──────────┬──────────┘  └──────────┬───────────┘  └────────────────────────┘  ║
╚═════════════╪═══════════════════════╪══════════════════════════════════════════╝
              │       ╔══════════════════════════════════════════════════════╗
              │       ║              SPEED LAYER (Spark Structured Streaming) ║
              │       ║                                                        ║
              └──────►║  ┌──────────────────────────────────────────────────┐ ║
                      ║  │  Job 1: orbit_enrichment.py                       │ ║
                      ║  │  ─ Schema normalization                           │ ║
                      ║  │  ─ Geo-enrichment (reverse geocode)               │ ║
                      ║  │  ─ Orbit classification (LEO/MEO/GEO/HEO)        │ ║
                      ║  │  ─ Sunlight detection                             │ ║
                      ║  │  Trigger: 15s  │  Watermark: 30s                 │ ║
                      ║  └────────────────┬─────────────────────────────────┘ ║
                      ║                   │ sat.position.enriched              ║
                      ║  ┌────────────────▼─────────────────────────────────┐ ║
                      ║  │  Job 2: anomaly_detection.py                      │ ║
                      ║  │  ─ VELOCITY_ANOMALY  (>2σ from expected)          │ ║
                      ║  │  ─ ALTITUDE_ANOMALY  (>5km change in 60s)         │ ║
                      ║  │  ─ SPACE_WEATHER_CORRELATION                      │ ║
                      ║  │  Trigger: 20s  │  Window: 60s                     │ ║
                      ║  └────────────────┬─────────────────────────────────┘ ║
                      ║                   │ sat.alerts                         ║
                      ╚═══════════════════╪════════════════════════════════════╝
                                          │
              ┌───────────────────────────┘
              │
              ▼                  ┌────── Also writes Parquet to HDFS every ~1 min
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         BATCH LAYER                                           ║
║                                                                               ║
║  ┌──────────────────────────────────────────────────────────────────────────┐ ║
║  │  HDFS (Hadoop 3.x)                                                       │ ║
║  │                                                                           │ ║
║  │  /satellite/raw/positions  ──── Parquet, partitioned date/satellite_id   │ ║
║  │  /satellite/raw/tle        ──── JSON, partitioned by date                │ ║
║  │  /satellite/raw/events     ──── JSON, partitioned date/event_type        │ ║
║  │  /satellite/aggregated/daily  ─ ORC (satellite_stats, country_stats,     │ ║
║  │                                        orbit_health)                     │ ║
║  │  /satellite/aggregated/weekly ─ ORC, weekly aggregates                   │ ║
║  │  /satellite/reports/drift  ──── MapReduce output, TLE drift analysis     │ ║
║  └───────────────────────────────────────────────────────────────────────────┘ ║
║                                                                               ║
║  ┌──────────────────────────────────────────────────────────────────────────┐ ║
║  │  Apache Airflow 2.8.1 (Orchestrator)                                     │ ║
║  │                                                                           │ ║
║  │  DAG: satellite_daily_pipeline     ── daily 02:00 UTC                    │ ║
║  │    check HDFS → Spark aggregation → Kafka trigger → Redis cache           │ ║
║  │                                                                           │ ║
║  │  DAG: satellite_weekly_pipeline    ── Sunday 04:00 UTC                   │ ║
║  │    ISO week calc → MapReduce TLE drift → Kafka trigger → Redis cache      │ ║
║  │                                                                           │ ║
║  │  DAG: satellite_monitoring         ── every 6 hours                      │ ║
║  │    freshness checks → alert on failure                                    │ ║
║  └───────────────────────────────────────────────────────────────────────────┘ ║
║                                                                               ║
║  ┌───────────────────────────┐  ┌────────────────────────────────────────── ┐ ║
║  │  Spark Batch Job          │  │  Hadoop MapReduce (TLE Drift Analysis)     │ ║
║  │  daily_aggregation.py     │  │  tle_drift_mapper.py                       │ ║
║  │  ─ satellite_stats        │  │  tle_drift_reducer.py                      │ ║
║  │  ─ country_stats          │  │  Week-over-week drift in:                  │ ║
║  │  ─ orbit_health           │  │    mean_motion, eccentricity, inclination  │ ║
║  └───────────┬───────────────┘  └──────────────┬─────────────────────────── ┘ ║
╚══════════════╪══════════════════════════════════╪══════════════════════════════╝
               │                                  │
               └────────────┬─────────────────────┘
                            │ Batch results written to Redis
                            ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                         SERVING LAYER                                           ║
║                                                                                  ║
║  ┌─────────────────────────────────────────────────────────────────────────┐    ║
║  │  Redis 7.2 (Hot Store / Cache)                                          │    ║
║  │                                                                          │    ║
║  │  Speed layer results:                                                    │    ║
║  │    sat:position:{norad_id}    ── latest enriched position (60s TTL)     │    ║
║  │    sat:pos:flat:{norad_id}    ── flat hash for Grafana (60s TTL)        │    ║
║  │    sat:meta:{norad_id}        ── metadata hash (1h TTL)                 │    ║
║  │    sat:alerts:{norad_id}      ── alert list (max 100)                   │    ║
║  │    alert:{alert_id}           ── full alert JSON (24h TTL)              │    ║
║  │    alerts:recent              ── 50 most recent alert JSONs             │    ║
║  │    event:{event_id}           ── space weather event JSON (72h TTL)     │    ║
║  │    events:active              ── active event ID set                    │    ║
║  │    events:recent              ── 50 most recent event JSONs             │    ║
║  │                                                                          │    ║
║  │  Batch layer results:                                                    │    ║
║  │    batch:daily:latest         ── latest daily aggregation (7d TTL)      │    ║
║  │    batch:daily:summary        ── flat hash of daily metrics (7d TTL)    │    ║
║  │    batch:daily:list           ── per-satellite rows for Grafana (7d TTL)│    ║
║  │    batch:weekly:latest        ── latest weekly drift JSON (7d TTL)      │    ║
║  │    batch:weekly:list          ── per-satellite drift records (7d TTL)   │    ║
║  │                                                                          │    ║
║  │  Pub/Sub:                                                                │    ║
║  │    channel:position:{norad_id} ── WebSocket streaming channel           │    ║
║  └─────────────────────────────────────────────────────────────────────────┘    ║
║                                                                                  ║
║  ┌──────────────────┐  ┌──────────────────────────┐  ┌───────────────────────┐ ║
║  │ kafka-redis-     │  │  FastAPI satellite-api   │  │  Grafana 10.2.3       │ ║
║  │ bridge           │  │  (port 8084)             │  │  (port 3000)          │ ║
║  │                  │  │                          │  │                       │ ║
║  │ Consumes:        │  │  REST + WebSocket        │  │  Redis datasource     │ ║
║  │  enriched pos    │  │  Merges speed+batch      │  │  Satellite Tracker    │ ║
║  │  alerts          │  │  results on query        │  │  dashboard            │ ║
║  │  events          │  │                          │  │  10s auto-refresh     │ ║
║  └──────────────────┘  └──────────────────────────┘  └───────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        ▼
                             ┌──────────────────────┐
                             │   End Users / APIs   │
                             │  Browsers, Dashboards│
                             └──────────────────────┘
```

---

## 4. Layer-by-Layer Explanation

### 4.1 Ingestion Layer (Data Sources)

The ingestion layer is composed of four concurrent Python producer processes. Each producer is responsible for a single external API, runs on its own polling interval, and publishes structured messages to a dedicated Kafka topic. All producers share a common implementation pattern: token bucket rate limiting (to respect API quotas), exponential backoff retry on transient errors, Pydantic schemas for runtime data validation, and loguru for structured logging.

#### ISSProducer
- **Source:** Open-Notify API (`http://api.open-notify.org/iss-now.json`)
- **Poll interval:** 5 seconds
- **Output topic:** `sat.position.raw`
- **Rationale:** Open-Notify is a free, reliable, unauthenticated API specifically designed for ISS position. Its 5-second resolution is sufficient for streaming telemetry. It provides latitude, longitude, and a Unix timestamp.

#### N2YOProducer
- **Source:** N2YO API (authenticated)
- **Poll interval:** 15 seconds
- **Satellites tracked:** ISS (NORAD 25544), Hubble (NORAD 20580), NOAA-20 (NORAD 43013)
- **Output topic:** `sat.position.raw`
- **Configuration:** Requires `N2YO_API_KEY` environment variable
- **Rationale:** N2YO covers a broader catalog and supports authenticated high-frequency queries. The 15-second interval balances data freshness against API rate limits. Publishing to the same topic as ISSProducer enables unified downstream processing; schema normalization in the speed layer reconciles the format differences.

#### DONKIProducer
- **Source:** NASA DONKI (Space Weather Database of Notifications, Knowledge, Information)
- **Poll interval:** 300 seconds (5 minutes)
- **Event types:** CME (Coronal Mass Ejection), FLR (Solar Flare), HSS (High-Speed Stream), and others
- **Output topic:** `sat.events.raw`
- **Rationale:** Space weather events are low-frequency but operationally significant. A 5-minute poll is appropriate since DONKI data is not real-time (it has hours of lag from the observation instruments). These events feed the anomaly correlation logic in the speed layer.

#### TLEProducer
- **Source:** TLE (Two-Line Element) API
- **Poll interval:** 3600 seconds (1 hour)
- **Output topic:** `sat.tle.raw`
- **Rationale:** TLE orbital element sets are updated infrequently (typically once or twice daily by NORAD). Hourly polling is sufficient to detect updates without over-fetching. TLE data feeds the batch-layer MapReduce drift analysis.

---

### 4.2 Message Bus (Kafka)

Apache Kafka serves as the central nervous system of the platform. It decouples producers from consumers, provides durable ordered message storage, and enables both the speed layer and batch layer to consume independently from the same data streams.

**Version:** `confluentinc/cp-kafka:7.5.3`, coordinated by Apache Zookeeper.

#### Topic Inventory

| Topic | Direction | Producers | Consumers | Purpose |
|---|---|---|---|---|
| `sat.position.raw` | Inbound | ISS, N2YO producers | Spark orbit_enrichment | Raw satellite positions |
| `sat.tle.raw` | Inbound | TLE producer | (archived to HDFS) | Orbital elements |
| `sat.events.raw` | Inbound | DONKI producer | kafka-redis-bridge | NASA space weather events |
| `sat.position.enriched` | Internal | Spark orbit_enrichment | Spark anomaly_detection, kafka-redis-bridge | Normalized, geo-enriched positions |
| `sat.alerts` | Internal | Spark anomaly_detection | kafka-redis-bridge | Detected anomalies |
| `sat.batch.trigger` | Control | Airflow DAGs | Downstream listeners | Batch completion signals |

**Why Kafka over alternatives (e.g., RabbitMQ, Pulsar)?**  
Kafka's log-based storage model is critical here. Both the speed layer and the batch layer need to replay messages from different offsets. Kafka retains messages durably and allows consumers to seek to arbitrary offsets — a capability that message queue systems like RabbitMQ do not provide. Kafka also provides the high throughput and horizontal scalability needed if the platform were to expand to tracking hundreds of satellites.

---

### 4.3 Speed Layer

The speed layer processes data with low latency, producing approximate but timely results. It compensates for its approximation by continuously updating its outputs; stale speed-layer results are replaced every 15–20 seconds.

The speed layer runs two chained Spark Structured Streaming jobs on an `apache/spark:3.5.1` cluster (1 master + 1 worker, 2 cores, 2 GB RAM).

#### Job 1: orbit_enrichment.py

This job reads raw position messages, normalizes their schema, and enriches them with derived orbital context.

**Input:** `sat.position.raw` (starting offset: latest)  
**Output:** `sat.position.enriched` (Kafka, every 15-second trigger) + HDFS Parquet (every ~1 minute, every 12 batches)  
**Checkpoint:** `hdfs://namenode:8020/satellite/checkpoints/enrichment`  
**Watermark:** 30 seconds (handles late-arriving messages)

**Transformations performed:**

1. **Schema normalization** — Open-Notify and N2YO have different JSON schemas. This step projects both formats into a common schema with standardized field names, units, and types.

2. **Geo-enrichment** — A bounding-box lookup table maps (latitude, longitude) pairs to country and region names. This is a static broadcast join (no external service calls) to keep latency predictable and avoid network dependencies.

3. **Orbit classification** — Using the altitude field and Kepler's laws, each position record is labeled with an orbit type:
   - LEO: Low Earth Orbit (160–2000 km)
   - MEO: Medium Earth Orbit (2000–35786 km)
   - GEO: Geostationary Orbit (~35786 km)
   - HEO: Highly Elliptical Orbit (variable altitude)
   
   Velocity and orbital period are also computed from the altitude, using the vis-viva equation and Kepler's third law respectively.

4. **Sunlight detection** — Solar elevation angle is calculated based on the satellite's position and UTC timestamp, determining whether the satellite is currently in sunlight or in Earth's shadow. This is used both for display and as input to the anomaly correlation logic.

**Why write to HDFS from the speed layer?**  
The speed layer's HDFS writes serve as the raw data input for the batch layer. Rather than having the batch layer re-consume from Kafka (which requires sufficient Kafka retention), the speed layer checkpoints its processed records to Parquet files in HDFS. This creates a clean separation: Kafka is the live bus, HDFS is the durable archive.

#### Job 2: anomaly_detection.py

This job reads enriched positions and detects three categories of anomalies in real time.

**Input:** `sat.position.enriched` (starting offset: latest)  
**Output:** `sat.alerts` (Kafka) + Redis hot store  
**Checkpoint:** `hdfs://namenode:8020/satellite/checkpoints/anomaly-alerts`  
**Trigger:** 20 seconds  
**Window:** 60 seconds

**Anomaly types detected:**

1. **VELOCITY_ANOMALY** — The satellite's observed speed deviates by more than 2 standard deviations from its expected orbital velocity. Expected velocity is derived from the altitude using vis-viva; deviations may indicate reporting errors, maneuver events, or anomalous orbital decay.

2. **ALTITUDE_ANOMALY** — The satellite's altitude changes by more than 5 km within a 60-second sliding window. For LEO satellites, normal altitude variation is typically under 1–2 km per pass. Larger changes indicate maneuvers or atmospheric drag events worth flagging.

3. **SPACE_WEATHER_CORRELATION** — A satellite is detected to be spatially within a zone affected by an active solar event (CME, flare, HSS) at the time of observation. This uses the event data previously written to Redis by the kafka-redis-bridge from `sat.events.raw`.

Alerts are written both to Kafka (for further consumption) and directly to Redis:
- `alert:{id}` — full alert JSON with 24-hour TTL
- `sat:alerts:{sat_id}` — list of alert IDs for each satellite (capped at 100)
- `alerts:recent` — the 50 most recent full alert JSONs, maintained as a Redis list for Grafana queries

---

### 4.4 Batch Layer

The batch layer processes the complete historical dataset with full accuracy. It runs on a schedule, hours after the data has been collected, and produces authoritative aggregations that complement the approximate speed-layer results.

#### Storage: HDFS (Hadoop 3.x)

HDFS provides distributed, fault-tolerant block storage for all raw and processed data. The directory layout is organized for both query efficiency (date and satellite partitioning minimizes scan ranges) and operational clarity:

```
/satellite/
  raw/
    positions/       ← Parquet, partitioned by date/satellite_id
    tle/             ← TLE JSON, partitioned by date
    events/          ← NASA events JSON, partitioned by date/event_type
  aggregated/
    daily/           ← ORC, daily summaries (satellite_stats, country_stats, orbit_health)
    weekly/          ← ORC, weekly aggregates
  reports/
    drift/           ← MapReduce output, TLE drift per week
  checkpoints/       ← Spark Structured Streaming checkpoints
/spark-logs/         ← Spark event logs
```

**Why ORC for aggregated data?**  
ORC (Optimized Row Columnar) format offers better compression and faster reads for analytical workloads than Parquet for wide tables with many columns. Raw data uses Parquet since it has better ecosystem support for streaming writes from Spark. Aggregated data uses ORC because it is read-heavy and column-oriented reads are the primary access pattern.

#### Orchestration: Apache Airflow 2.8.1

Airflow orchestrates all batch pipelines. It uses the LocalExecutor (single-machine task execution) backed by a PostgreSQL database for metadata and state management.

**DAG 1: satellite_daily_pipeline**
- Schedule: Daily at 02:00 UTC
- Steps: Check HDFS data exists → run Spark daily aggregation → publish completion signal to `sat.batch.trigger` → cache aggregation results to Redis
- The 02:00 UTC schedule gives Spark Streaming sufficient time to write all previous day's Parquet files to HDFS (the last files typically land by midnight).

**DAG 2: satellite_weekly_pipeline**
- Schedule: Sunday at 04:00 UTC
- Steps: Calculate ISO week number → run Hadoop MapReduce TLE drift analysis → publish to `sat.batch.trigger` → cache drift results to Redis
- The Sunday schedule ensures a full week of TLE samples is available before the drift analysis runs.

**DAG 3: satellite_monitoring**
- Schedule: Every 6 hours
- Steps: Freshness checks on HDFS positions and aggregations; alert on any check failure
- This is a data quality and SLA monitoring pipeline, not a processing pipeline. It verifies that the speed and batch layers are producing data within expected time bounds.

#### Batch Job 1: daily_aggregation.py (Spark)

**Input:** `/satellite/raw/positions/date={date}` (Parquet)  
**Outputs:**
- ORC tables at `/satellite/aggregated/daily/date={date}/satellite_stats`
- ORC tables at `/satellite/aggregated/daily/date={date}/country_stats`
- ORC tables at `/satellite/aggregated/daily/date={date}/orbit_health`
- Redis hash `batch:daily:summary` (flat metrics for API queries)
- Redis list `batch:daily:list` (per-satellite rows for Grafana panels)

**Computed metrics:**

- `satellite_stats` — per-satellite: total position records, min/max/avg altitude, percentage of time in sunlight, set of countries overflown
- `country_stats` — per-satellite per-country: number of overflights, total time over each country
- `orbit_health` — per-satellite: altitude decay rate (linear regression over the day), stability score (variance in altitude), decay anomaly flag

#### Batch Job 2: TLE Drift MapReduce (Hadoop Streaming)

This job uses Hadoop Streaming to run Python mapper and reducer scripts on TLE data.

**Mapper (tle_drift_mapper.py):**  
Parses raw TLE records from HDFS, extracts key orbital elements (mean motion, eccentricity, inclination, RAAN, argument of perigee), and emits key-value pairs: `satellite_id → serialized_orbital_elements`.

**Reducer (tle_drift_reducer.py):**  
Groups records by satellite ID. For each satellite, it computes week-over-week drift in:
- Mean motion (revolutions per day) — drift indicates orbital altitude change
- Eccentricity — drift indicates orbit shape change (circularization or elongation)
- Inclination — drift indicates orbital plane shift (rare but significant)

Anomaly flags are set when drift exceeds configurable thresholds. Output is written as JSON lines to `/satellite/reports/drift/week={week_number}`.

**Why MapReduce for TLE drift instead of Spark?**  
MapReduce is deliberately used here to demonstrate the Hadoop ecosystem alongside Spark. TLE drift analysis is a naturally map-reduce-shaped problem: map each TLE record to its satellite, reduce by satellite to compute aggregate drift. The data volume is modest (a few hundred records per week for three satellites), so MapReduce is not chosen for performance but for architectural demonstration.

---

### 4.5 Serving Layer

The serving layer is the point where the speed and batch layers converge. It exposes both real-time streaming data (from the speed layer) and historical batch results (from the batch layer) through a unified API surface. Redis acts as the shared in-memory store that both layers write to, and that the API and Grafana read from.

#### Redis 7.2 (Hot Store / Cache)

Redis serves as the materialized view of all serving-layer data. The kafka-redis-bridge continuously writes speed-layer results to Redis; the Airflow batch DAGs write batch-layer results to Redis on completion.

**Key schema and TTL design:**

The TTL strategy encodes the freshness contract of each data type:

| Key Pattern | TTL | Rationale |
|---|---|---|
| `sat:position:{norad_id}` | 60 seconds | A position older than 60s is considered stale for real-time display |
| `sat:pos:flat:{norad_id}` | 60 seconds | Same; flat hash format optimized for Grafana queries |
| `sat:meta:{norad_id}` | 1 hour | Satellite metadata (orbit type, name) changes infrequently |
| `alert:{alert_id}` | 24 hours | Alerts are operationally relevant for one day |
| `event:{event_id}` | 72 hours | Space weather events last up to several days |
| `batch:daily:*` | 7 days | Daily aggregations are valid until superseded |
| `batch:weekly:*` | 7 days | Weekly drift reports are valid for the next reporting cycle |

**Pub/Sub channels:**  
`channel:position:{norad_id}` is a Redis pub/sub channel. The kafka-redis-bridge publishes to this channel whenever it writes a new position for a satellite. The FastAPI WebSocket endpoint subscribes to this channel and forwards messages to connected WebSocket clients, enabling real-time position streaming with no polling.

#### kafka-redis-bridge (Custom Python Service)

This service is the bridge between the Kafka message bus and Redis. It consumes from three Kafka topics:
- `sat.position.enriched` — writes position keys, flat keys, metadata keys, pub/sub channel
- `sat.alerts` — writes alert keys, per-satellite alert lists, `alerts:recent` list
- `sat.events.raw` — writes event keys, active event set, `events:recent` list

On startup, the bridge performs a backfill: it scans all existing `event:*` keys in Redis and rebuilds the `events:recent` list, ensuring that the display list is consistent after a service restart.

The bridge uses `python-snappy` for Kafka message decompression, which provides approximately 50–70% size reduction for JSON payloads.

#### FastAPI satellite-api (Port 8084)

The API layer merges speed-layer and batch-layer results. It reads exclusively from Redis; it does not query Kafka, HDFS, or any database directly. This design keeps latency low (sub-millisecond Redis reads) and isolates the API from the complexity of the processing layers.

**Endpoint inventory:**

| Method | Path | Description | Data Source |
|---|---|---|---|
| GET | `/api/satellites` | List all tracked satellites with live status | Redis speed layer |
| GET | `/api/satellites/{id}/position` | Latest position (404 if older than 60s) | Redis speed layer |
| GET | `/api/satellites/{id}/alerts` | Recent anomaly alerts for a satellite | Redis speed layer |
| GET | `/api/events/active` | Active space weather events | Redis speed layer |
| GET | `/api/reports/daily/{date}` | Daily batch aggregation report | Redis batch layer |
| GET | `/api/reports/drift/{week}` | Weekly TLE drift report | Redis batch layer |
| WS | `/ws/position/{satellite_id}` | Real-time position stream | Redis pub/sub |
| GET | `/health` | Liveness probe | In-process check |

The `/api/satellites/{id}/position` endpoint is the clearest example of Lambda merging: if a position key exists in Redis (speed layer, 60s TTL) it is returned as the live result. If not, the API can fall back to the latest batch result. The 60-second TTL on position keys means the API naturally surfaces staleness — if the speed layer stops writing, the API starts returning 404, alerting clients to a degraded state.

#### Grafana 10.2.3 (Port 3000)

Grafana provides the operational dashboard. It connects directly to Redis using the `redis-datasource` plugin, bypassing the FastAPI layer entirely for dashboard queries. This avoids adding FastAPI latency to Grafana's 10-second auto-refresh cycle.

The Satellite Tracker dashboard is auto-provisioned from a JSON definition at container startup, so it is available immediately after `docker compose up`. All panels query Redis keys directly — there is no aggregation logic in Grafana itself; Grafana only renders data that has already been materialized into Redis by either the speed layer or the batch layer.

---

## 5. Data Flow Walkthrough — One Position Message End to End

This section traces a single ISS position message from its origin at the Open-Notify API through every component in the system.

### T+0s — ISSProducer polls Open-Notify

The ISSProducer fires its 5-second poll timer. It sends an HTTP GET request to `http://api.open-notify.org/iss-now.json`. The API responds with:

```json
{
  "timestamp": 1716892800,
  "message": "success",
  "iss_position": { "latitude": "51.6742", "longitude": "-12.4891" }
}
```

The producer validates this against its Pydantic schema, wraps it in an envelope (adding source identifier, ingestion timestamp, satellite NORAD ID), and publishes it to the `sat.position.raw` Kafka topic. The Kafka produce call is non-blocking; the producer returns immediately.

### T+0s–T+5s — Message sits in Kafka

The message is written to a partition of `sat.position.raw`. Kafka durably stores it on disk. Two consumers are registered on this topic: Spark orbit_enrichment.py. The message waits in the partition until the next Spark micro-batch trigger.

### T+0s–T+15s — Spark orbit_enrichment triggers

Every 15 seconds, orbit_enrichment.py's Spark Structured Streaming trigger fires. It reads all messages accumulated in `sat.position.raw` since the previous trigger.

For the ISS position message:

1. **Schema normalization** — The Open-Notify schema is projected to the canonical schema. Fields are renamed and typed: `iss_position.latitude` → `latitude: Float`, `iss_position.longitude` → `longitude: Float`, `timestamp` → `event_time: Timestamp`.

2. **Geo-enrichment** — The latitude 51.67, longitude -12.49 is looked up in the bounding box table. This maps to the North Atlantic Ocean (no country, region: "Atlantic Ocean"). The `country` field is set to `null`, `region` to `"Atlantic Ocean"`.

3. **Orbit classification** — The ISS orbits at approximately 408 km altitude. Using vis-viva: velocity ≈ 7.66 km/s, period ≈ 92.7 minutes. Orbit type: LEO.

4. **Sunlight detection** — Based on position and UTC timestamp, the solar elevation angle above the ISS horizon is computed. Assume the result is +12°: the ISS is in sunlight.

The enriched record is published to `sat.position.enriched` on Kafka. A copy is buffered; after every 12 micro-batches (~1 minute) the buffer is flushed to HDFS as a Parquet file at `/satellite/raw/positions/date=2026-05-28/satellite_id=25544/part-*.parquet`.

### T+15s–T+35s — Spark anomaly_detection triggers

The enriched message arrives at `sat.position.enriched`. anomaly_detection.py triggers every 20 seconds. Within its 60-second sliding window, it:

1. Computes expected velocity for 408 km altitude: 7.66 km/s. The reported velocity is within 2σ: no VELOCITY_ANOMALY.
2. Compares current altitude to the previous record within the window: change is 0.3 km. Threshold is 5 km: no ALTITUDE_ANOMALY.
3. Checks Redis `events:active`: there are no active solar events intersecting the ISS ground track at this time: no SPACE_WEATHER_CORRELATION.

No alert is generated. The enriched position record proceeds to the kafka-redis-bridge.

### T+15s–T+20s — kafka-redis-bridge writes Redis

The kafka-redis-bridge consumes the enriched position from `sat.position.enriched`. It writes:

- `sat:position:25544` — full enriched JSON, TTL 60s
- `sat:pos:flat:25544` — flat hash (latitude, longitude, altitude, velocity, orbit_type, in_sunlight, country, region), TTL 60s
- `sat:meta:25544` — hash with name="ISS", norad_id=25544, orbit_type="LEO", last_seen=now, TTL 1h
- Publishes to `channel:position:25544` (Redis pub/sub)

### T+20s — FastAPI WebSocket client receives position

Any WebSocket client connected to `/ws/position/25544` is subscribed via the FastAPI service to `channel:position:25544`. The pub/sub message arrives and is immediately forwarded to the WebSocket client. From the original API poll at T+0s, the client sees the position in approximately 15–20 seconds.

### T+20s — Grafana refreshes

On the next 10-second Grafana auto-refresh, panels querying `sat:pos:flat:25544` see the new position. The ISS position on the map panel updates.

### T+02:00 UTC (next day) — Airflow daily batch runs

The daily batch pipeline starts. It reads the Parquet files written to `/satellite/raw/positions/date=2026-05-28/satellite_id=25544/` — including the file containing the ISS position from T+1min. Spark computes `satellite_stats`, `country_stats`, and `orbit_health` for that day. The results are written to ORC in HDFS and cached in Redis under `batch:daily:*` keys. The daily report for 2026-05-28 is now available via `GET /api/reports/daily/2026-05-28`.

### Summary of Latency Profile

| Milestone | Latency from API poll |
|---|---|
| Message published to Kafka | < 1 second |
| Spark enrichment complete | 0–15 seconds |
| Redis updated | 15–20 seconds |
| Grafana reflects update | 15–30 seconds |
| WebSocket client notified | 15–20 seconds |
| Daily batch aggregation available | ~24 hours |

---

## 6. Key Design Decisions

### Why Lambda Architecture?

Lambda Architecture is appropriate when the system must satisfy two conflicting requirements simultaneously: **low-latency approximate answers** for real-time monitoring, and **high-accuracy complete answers** for historical analysis and reporting.

For satellite tracking:
- Operations teams need to know where the ISS is *now* (speed layer, seconds latency).
- Analysts need to know the ISS's daily ground track statistics for the past month (batch layer, complete data).
- A kappa architecture (streaming only) would require reprocessing historical data through the stream processor, which is complex and resource-intensive at scale.
- A batch-only architecture would have unacceptable latency for real-time monitoring.

Lambda keeps the two concerns cleanly separated and independently scalable.

### Why Kafka as the Central Bus?

Kafka's log-based retention model is the key architectural enabler. It allows:
- **Replay:** The batch layer can reprocess historical data by rewinding the Kafka consumer offset.
- **Fan-out:** Multiple independent consumers (speed layer, archival, monitoring) consume the same topic without coordination.
- **Back-pressure:** Producers are decoupled from consumers; bursts in production rate are absorbed by the log.
- **Durability:** Messages are persisted to disk, surviving consumer failures.

### Why Spark Structured Streaming?

Spark Structured Streaming provides a unified API for both batch and streaming processing. This is architecturally significant: the enrichment logic in orbit_enrichment.py can be tested and debugged in batch mode, and the same code runs in streaming mode. The watermark mechanism handles late-arriving messages gracefully, which is critical when dealing with external APIs that may have variable response latency.

The 15-second micro-batch trigger is a deliberate latency/throughput trade-off. Sub-second latency is not required for satellite tracking; 15 seconds provides sufficient freshness while reducing per-batch overhead.

### Why Hadoop MapReduce for TLE Drift?

Hadoop MapReduce is included explicitly to demonstrate the traditional Hadoop ecosystem alongside the modern Spark stack. The TLE drift analysis is a computationally simple, naturally map-reduce-shaped problem, making it a clean fit for MapReduce without over-engineering. In a production system, this job would likely be replaced with Spark for operational simplicity.

### Why Redis as the Serving Store?

Redis provides sub-millisecond read latency, which is appropriate for a serving layer that must respond to API requests in real time. Its data structures (hashes, lists, sets, pub/sub channels) map directly to the access patterns of the serving layer:
- Hashes for structured position and metadata records
- Lists for ordered alert and event histories (capped with `LPUSH`/`LTRIM`)
- Sets for active event membership queries
- Pub/sub for WebSocket push notifications

TTL-based expiry is used to implement data freshness semantics without requiring explicit cache invalidation logic.

### Why FastAPI?

FastAPI provides async I/O, which is critical for the WebSocket endpoint that maintains long-lived connections while simultaneously processing Redis pub/sub messages. Its automatic OpenAPI documentation is a development convenience. The async Redis client (`aioredis`) integrates naturally with the FastAPI async event loop.

### Why Grafana with Redis Datasource?

Grafana's Redis datasource plugin allows dashboards to query Redis directly, bypassing the FastAPI layer. This is architecturally cleaner than routing dashboard queries through the API, which would add network hops and increase API load during dashboard refreshes. The data has already been materialized into Redis by the processing layers, so Grafana's role is purely visualization.

---

## 7. Technology Stack Table

| Component | Technology | Version | Role |
|---|---|---|---|
| Data producers | Python | 3.11 | External API polling, Kafka publishing |
| Schema validation | Pydantic | v2 | Runtime data validation in producers |
| Logging | loguru | latest | Structured logging in producers |
| Message bus | Apache Kafka | 7.5.3 (Confluent) | Durable ordered message streaming |
| Kafka coordination | Apache Zookeeper | bundled | Kafka broker coordination |
| Stream processing | Apache Spark (Structured Streaming) | 3.5.1 | Real-time enrichment and anomaly detection |
| Batch processing | Apache Spark (batch) | 3.5.1 | Daily statistical aggregations |
| MapReduce | Hadoop Streaming (Python) | 3.x | TLE orbital drift analysis |
| Distributed storage | HDFS (NameNode + DataNode) | 3.x | Durable archival of raw and processed data |
| Orchestration | Apache Airflow | 2.8.1 | Batch pipeline scheduling and monitoring |
| Airflow backend | PostgreSQL | latest | Airflow metadata and state |
| Hot store / cache | Redis | 7.2 | Serving layer materialized views |
| Redis-Kafka bridge | Custom Python service | — | Consumes Kafka, writes Redis |
| REST + WebSocket API | FastAPI | latest | Unified serving layer API |
| Dashboard | Grafana | 10.2.3 | Operational visualization |
| Grafana datasource | redis-datasource plugin | latest | Direct Redis queries from Grafana |
| Container runtime | Docker Compose | — | All services on `satellite-net` bridge network |
| Message compression | python-snappy | latest | Kafka payload compression in bridge |

---

## 8. Limitations and Known Constraints

### Single-Node HDFS
The HDFS cluster runs as a single NameNode + single DataNode. There is no NameNode high-availability, no secondary NameNode, and no data replication beyond the single DataNode. This means:
- A DataNode failure results in data loss.
- A NameNode failure makes the entire filesystem unavailable.

This is acceptable for a demonstration platform but must be addressed (at minimum: HDFS replication factor ≥ 2, standby NameNode) before any production use.

### Spark Cluster Resource Constraints
The Spark cluster is allocated 2 cores and 2 GB RAM (1 master + 1 worker). This is sufficient for three satellites at their current polling frequencies. Adding more satellites or reducing polling intervals will exhaust worker resources. The cluster would need to be scaled horizontally (additional workers) or vertically (more RAM/CPU per worker) before the tracking scope can be expanded significantly.

### No Authentication or Authorization
All services — Kafka, HDFS, Airflow, FastAPI, Grafana, Redis — run without authentication. There is no TLS in transit. This is intentional for development simplicity but makes the platform completely unsuitable for any multi-tenant or internet-exposed deployment.

### N2YO API Rate Limits
The N2YO API enforces rate limits based on API key tier. The free tier allows a limited number of transactions per hour. At 15-second polling for 3 satellites, the platform consumes approximately 240 API calls per hour. Exceeding the quota results in API errors, which the exponential backoff retry in the producer handles gracefully, but a sustained quota breach will result in stale N2YO data.

### Kafka Retention and Batch Replay
The platform relies on Spark Streaming writing position data to HDFS for batch processing, rather than the batch layer replaying from Kafka. This means that if the HDFS write from Spark Streaming fails (e.g., due to HDFS unavailability), those records are lost to the batch layer even if they remain available in Kafka. The Kafka retention period is not explicitly configured for long-term batch replay in this setup.

### Airflow LocalExecutor Scalability
Airflow is configured with LocalExecutor, which runs tasks as local subprocess. This means all Airflow tasks run on the Airflow container's host machine. Adding more DAGs or parallelizing more tasks will eventually exhaust the host's CPU and memory. For production, the CeleryExecutor or KubernetesExecutor would be required.

### Speed Layer Approximations
The speed layer uses 30-second watermarks and 15–20 second micro-batch triggers. Messages arriving more than 30 seconds late are dropped by the watermark mechanism. In practice, the polling APIs have consistent sub-second response times, so late arrival is unlikely but not impossible during API degradation events.

### Redis Single-Instance
Redis runs as a single instance with no replication or persistence configured beyond what Docker volumes provide. A Redis process crash and container restart will lose all in-memory data (all position keys, alert lists, event sets). The kafka-redis-bridge will repopulate position and event data as new messages arrive from Kafka, but there is a cold-start period during which the serving layer returns empty results. The batch layer data in Redis (with 7-day TTL) is similarly vulnerable.

### Grafana Data Source Coupling
Grafana queries Redis keys directly by name. The key schema is therefore a shared contract between the kafka-redis-bridge (writer) and Grafana (reader). Any change to Redis key naming or data structure in the bridge requires a matching update to Grafana panel queries. This tight coupling is manageable at small scale but becomes a maintenance burden as the number of panels grows.

---

*End of Architecture Documentation*
