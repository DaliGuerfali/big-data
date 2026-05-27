# Satellite Tracking Big Data Platform

Real-time satellite tracking platform demonstrating Lambda Architecture with Hadoop, Spark, and Kafka.

---

## Prerequisites

You only need **two things** installed on your machine before running anything:

### 1. Docker Desktop
Everything (Hadoop, Spark, Kafka, Redis, Airflow, Grafana) runs inside Docker — no manual installation needed.

- Download: https://www.docker.com/products/docker-desktop/
- Minimum resources to allocate in Docker Desktop settings:
  - **CPUs:** 4
  - **Memory:** 8 GB
  - **Disk:** 20 GB

Verify it works:
```powershell
docker --version        # Docker Desktop 4.x or higher
docker compose version  # v2.x or higher
```

### 2. Python 3.10+
Only needed for the Kafka producers (Part 2). Not required just to start the infrastructure.

- Download: https://www.python.org/downloads/
- Verify: `python --version`

---

## Quick Start

### Step 1 — Clone / open the project
```powershell
cd "C:\Users\Dali\Desktop\Projet Big Data"
```

### Step 2 — Configure environment
```powershell
copy .env.example .env
# Open .env and fill in at minimum your N2YO_API_KEY
# NASA_API_KEY can stay as DEMO_KEY for now
```

### Step 3 — Start the platform
```powershell
.\scripts\start.ps1
```

This single command will, in order:
1. Start Zookeeper and Kafka, wait until healthy
2. Create all 6 Kafka topics with correct partitions and retention
3. Start Hadoop NameNode, wait until healthy, then start DataNode
4. Create all HDFS directories (`/satellite/raw`, `/satellite/aggregated`, etc.)
5. Start Spark Master + Worker
6. Start Redis
7. Initialize Airflow database and admin user
8. Start Airflow Webserver + Scheduler
9. Start Grafana

First run takes **5-10 minutes** (Docker pulls images). Subsequent starts take ~1 minute.

### Step 4 — Verify everything is up
```powershell
.\scripts\health-check.ps1
```

### Step 5 — Install Python dependencies (for producers)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| HDFS NameNode UI | http://localhost:9870 | — |
| Spark Master UI | http://localhost:8081 | — |
| Airflow | http://localhost:8083 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

---

## Stop the Platform

```powershell
.\scripts\stop.ps1          # Stop containers, keep all data
.\scripts\stop.ps1 -Clean   # Stop and delete all volumes (full reset)
```

---

## What start.ps1 Does NOT Do

| Task | How to do it |
|------|-------------|
| Install Docker Desktop | Manual — see Prerequisites above |
| Install Python | Manual — see Prerequisites above |
| Install Python packages | `pip install -r requirements.txt` |
| Start Kafka producers | `python producers/main.py` (Part 2) |
| Submit Spark jobs | `spark-submit ...` (Part 3 & 4) |

---

## Architecture

```
APIs (ISS, N2YO, DONKI, TLE)
        │
        ▼
Kafka Producers (Python)
        │
        ▼
Kafka Topics
  ├── sat.position.raw  (6 partitions, 1h retention)
  ├── sat.tle.raw       (3 partitions, 24h retention)
  ├── sat.events.raw    (3 partitions, 72h retention)
  ├── sat.position.enriched (6 partitions, 6h retention)
  ├── sat.alerts        (2 partitions, 24h retention)
  └── sat.batch.trigger (1 partition,  7d retention)
        │
        ├──► Spark Structured Streaming
        │     ├── Op1: Geo-enrichment → sat.position.enriched + HDFS
        │     └── Op2: Anomaly detection → sat.alerts + Redis
        │
        └──► Spark Batch + MapReduce (Airflow-triggered)
              ├── Daily: Orbital pass aggregation → HDFS ORC
              └── Weekly: TLE drift analysis → HDFS JSON
                            │
                            ▼
                    Serving Layer
                  Redis │ FastAPI │ Grafana
```

## Implementation Parts

| Part | Status | Description |
|------|--------|-------------|
| 1 — Infrastructure | Done | Docker, Kafka, Hadoop, Spark |
| 2 — Producers | Done | Python Kafka producers for all 4 APIs |
| 3 — Stream Processing | Done | Spark Structured Streaming jobs |
| 4 — Batch Processing | Done | Spark + MapReduce jobs |
| 5 — Orchestration | Done | Airflow DAGs |
| 6 — Serving Layer | Done | FastAPI, Redis bridge, Grafana |
| 7 — Testing | Pending | Integration and performance tests |

---

## Running the Pipelines (Part 5)

### How it works

Airflow cannot run `spark-submit` or `hadoop` itself — it doesn't have those binaries.
Instead, the custom `SparkSubmitDockerOperator` and `HadoopStreamingDockerOperator`
(in `airflow/plugins/operators/`) exec commands inside the **running** `spark-master`
and `namenode` containers via the Docker socket, streaming all output back to the
Airflow task log.

### DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `satellite_daily_pipeline` | 02:00 UTC daily | HDFS freshness check → Spark aggregation → trigger |
| `satellite_weekly_pipeline` | 04:00 UTC Sundays | ISO week calc → Hadoop MapReduce TLE drift → trigger |
| `satellite_monitoring` | Every 6 hours | HDFS partition freshness alerts |

### Trigger a DAG manually

```powershell
# From the Airflow UI at http://localhost:8083 — toggle the DAG on, then click "Trigger DAG"
# Or via CLI:
docker exec airflow-scheduler airflow dags trigger satellite_daily_pipeline --conf '{"date":"2024-01-15"}'
```

### Running tests

```powershell
# Pure-logic tests (no Docker or Airflow needed):
.venv\Scripts\python.exe -m pytest tests/test_docker_exec_operator.py tests/test_week_boundaries.py -v

# Full suite including DAG-validity tests (run inside the container where Airflow is installed):
docker exec airflow-scheduler python -m pytest /opt/airflow/tests/ -v
```
