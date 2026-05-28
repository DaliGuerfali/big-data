# Running the Satellite Tracking Platform

This document is the authoritative runbook for the Big Data satellite tracking platform. Follow it top-to-bottom for a first-time setup, or jump to the relevant section for day-to-day operations.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Initial Setup (one-time)](#2-initial-setup-one-time)
3. [Starting the Platform](#3-starting-the-platform)
4. [Stopping the Platform](#4-stopping-the-platform)
5. [Service URLs](#5-service-urls)
6. [Starting the Data Pipeline](#6-starting-the-data-pipeline)
7. [Batch Processing (Airflow DAGs)](#7-batch-processing-airflow-dags)
8. [Health Verification](#8-health-verification)
9. [Verifying Data is Flowing](#9-verifying-data-is-flowing)
10. [Troubleshooting](#10-troubleshooting)
11. [Complete Demo Sequence](#11-complete-demo-sequence)
12. [Tips and Notes](#12-tips-and-notes)

---

## 1. Prerequisites

### Required Software

| Software | Minimum Version | Notes |
|---|---|---|
| Docker Desktop (Windows) | Latest stable | Must have at least **8 GB RAM** allocated in Settings → Resources |
| Python | 3.11+ | Used for producers and local tooling |
| PowerShell | 5.1+ | Ships with Windows 10/11 |

To verify your installations:

```powershell
docker --version
docker compose version
python --version
$PSVersionTable.PSVersion
```

### Required API Keys

| Key | Required | Where to Get |
|---|---|---|
| `N2YO_API_KEY` | **Yes** | Register for free at [n2yo.com](https://www.n2yo.com/api/) |
| `NASA_API_KEY` | No | [api.nasa.gov](https://api.nasa.gov/) — `DEMO_KEY` works but is heavily rate-limited (30 req/hour) |

> **Docker RAM warning:** If Docker Desktop is allocated less than 8 GB, Spark and Airflow containers will crash with out-of-memory errors. Go to Docker Desktop → Settings → Resources → Memory and set it to at least 8192 MB before proceeding.

---

## 2. Initial Setup (one-time)

Run these steps once on a fresh clone. You do not need to repeat them on subsequent runs unless you wipe volumes.

### Step 1 — Copy and configure the environment file

```powershell
Copy-Item .env.example .env
```

Open `.env` in a text editor and fill in:

```
N2YO_API_KEY=your_key_here
NASA_API_KEY=your_key_or_DEMO_KEY
```

### Step 2 — Create the Python virtual environment

```powershell
python -m venv .venv
.venv\Scripts\activate
```

Your prompt should now show `(.venv)` at the start.

### Step 3 — Install Python dependencies

```powershell
pip install -r requirements.txt
```

### Step 4 — Build all Docker images

```powershell
docker compose -f docker\docker-compose.yml build
```

This step takes several minutes on first run as Docker downloads base images. Subsequent builds are cached and much faster.

> **Tip:** If you only changed one service (e.g., `satellite-api`), you can rebuild just that service: `docker compose -f docker\docker-compose.yml build satellite-api`

---

## 3. Starting the Platform

### Full Start (recommended)

```powershell
.\scripts\start.ps1
```

The script starts services in dependency order, waiting for each layer to become healthy before proceeding. It performs 6 sequential steps:

| Step | Services Started | Wait Condition |
|---|---|---|
| 1 | Zookeeper, Kafka | Kafka healthy (up to 120 s) |
| 2 | Kafka topics initialization | One-off container — waits for completion |
| 3 | Hadoop NameNode, DataNode | NameNode healthy (up to 180 s) |
| 4 | HDFS directories initialization | One-off container — waits for completion |
| 5 | Spark Master, Spark Worker, Redis, Kafka UI | Each waits up to 60 s |
| 6 | PostgreSQL, Airflow init, Airflow webserver, Airflow scheduler, Grafana, satellite-api, kafka-redis-bridge | All started together |

Total expected startup time: **3–5 minutes** on first run, **1–2 minutes** on subsequent starts.

### Full Reset (wipe all data and restart)

```powershell
.\scripts\start.ps1 -Reset
```

You will be prompted to type `yes` to confirm. This **deletes all Docker volumes** — all Kafka offsets, HDFS data, Redis keys, PostgreSQL data, and Airflow history will be permanently erased. Use this when you want a completely clean slate.

---

## 4. Stopping the Platform

### Stop containers only (data is preserved)

```powershell
docker compose -f docker\docker-compose.yml down
```

### Stop containers and delete all data volumes

```powershell
docker compose -f docker\docker-compose.yml down -v
```

> **Note:** After a `down -v`, the next start is equivalent to a first-time setup. Re-run `.\scripts\start.ps1` (not the one-time setup steps in section 2, which only need to be done once per machine).

---

## 5. Service URLs

All services are accessible on `localhost` after startup.

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | None |
| HDFS NameNode UI | http://localhost:9870 | None |
| Spark Master UI | http://localhost:8081 | None |
| Spark Worker UI | http://localhost:8082 | None |
| Airflow | http://localhost:8083 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Satellite API | http://localhost:8084 | None |
| API Docs (Swagger) | http://localhost:8084/docs | None |

---

## 6. Starting the Data Pipeline

The data pipeline consists of two parts: the **speed layer** (real-time Spark Streaming jobs + producers) and the **batch layer** (Airflow DAGs). Start the speed layer first.

### Start Spark Streaming Jobs and Producers

```powershell
.\scripts\run_streaming.ps1
```

This script performs the following actions automatically:

1. Opens a new PowerShell window and starts all four producers concurrently:
   - ISS producer (real-time ISS position from open-notify.org)
   - N2YO producer (tracked satellite positions)
   - DONKI producer (NASA space weather events)
   - TLE producer (Two-Line Element orbital data)
2. Submits `orbit_enrichment.py` to Spark (reads `sat.position.raw` → writes `sat.position.enriched` + HDFS)
3. Installs `redis` pip package if missing, then submits `anomaly_detection.py` to Spark (reads `sat.position.enriched` → writes `sat.alerts` + Redis)

#### First-run note — Maven package download

On the very first run, Spark must download Kafka integration JARs from Maven Central. This takes **3–5 minutes**. The Spark UI at http://localhost:8081 will show 0 Running Applications during this time. This is normal — wait for it.

After the packages are cached locally, subsequent runs start in under 30 seconds.

#### Verify streaming is working

Open http://localhost:8081. You should see exactly **2 Running Applications**:
- `SatOrbitEnrichment`
- `SatAnomalyDetection`

Open http://localhost:3000 (Grafana). The live ISS position panel should update within **30 seconds** of the producers starting.

### Reset Streaming (clear checkpoints and resubmit)

If Spark jobs crash or produce stale data:

```powershell
.\scripts\run_streaming.ps1 -Reset
```

This clears Spark checkpoints and resubmits both jobs from scratch.

### Run Producers Only (if streaming jobs are already running)

If you need to restart only the producers without resubmitting Spark jobs:

```powershell
# Must be run from the project root with .venv activated
.venv\Scripts\activate
python -m producers.main
```

> **Common pitfall:** Do **not** run `python producers/main.py`. Python will throw a relative import error (`attempted relative import with no known parent package`). Always use `python -m producers.main` from the project root.

---

## 7. Batch Processing (Airflow DAGs)

Batch pipelines run on a schedule but can be triggered manually at any time. Access Airflow at http://localhost:8083 (credentials: `admin` / `admin`).

### Daily Pipeline — `satellite_daily_pipeline`

- **Scheduled:** Every day at 02:00 UTC
- **Purpose:** Aggregates orbital data from HDFS for the past 24 hours

**To trigger manually:**

1. Open http://localhost:8083
2. Locate `satellite_daily_pipeline` in the DAG list
3. Toggle the pause switch to **unpause** the DAG (blue = active)
4. Click the **play button (▶)** → select "Trigger DAG"
5. Monitor progress in the **Graph view**

**Task sequence:**

```
check_hdfs_data → daily_orbital_aggregation → publish_batch_trigger → cache_results_to_redis
```

**Expected duration:** 2–3 minutes.

**Result:** Grafana panel "Daily Aggregation" will show data after completion.

### Weekly Pipeline — `satellite_weekly_pipeline`

- **Scheduled:** Every Sunday at 04:00 UTC
- **Purpose:** Runs TLE drift MapReduce job over the past week

**To trigger manually:**

1. Unpause `satellite_weekly_pipeline`
2. Trigger manually (same procedure as daily pipeline)

**Task sequence:**

```
calculate_week_range → tle_drift_mapreduce → publish_batch_trigger → cache_results_to_redis
```

**Expected duration:** 2–3 minutes.

**Result:** Grafana panel "Weekly TLE Drift" will show data after completion.

### Monitoring Pipeline — `satellite_monitoring`

- **Scheduled:** Every 6 hours
- **Purpose:** Checks data freshness and alerts on missing data
- **Note:** Safe to leave paused if you do not need monitoring alerts.

---

## 8. Health Verification

### Quick check with the health script

```powershell
.\scripts\health-check.ps1
```

### Manual health checks

**All containers running:**

```powershell
docker ps
```

Every container listed in `docker\docker-compose.yml` should have status `Up` or `Up (healthy)`. Any container in `Restarting` or `Exited` state needs attention — see section 10.

**Redis responding:**

```powershell
docker exec redis redis-cli PING
```

Expected output: `PONG`

**Kafka broker responding:**

```powershell
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

Expected output: a list of Kafka API versions (several dozen lines).

**Satellite API health endpoint:**

```powershell
curl http://localhost:8084/health
```

Expected output: `{"status":"ok"}`

---

## 9. Verifying Data is Flowing

Run these checks after starting the full pipeline (infrastructure + streaming jobs + producers).

### Kafka — check consumer group lag

```powershell
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group redis-bridge
```

The `LAG` column should show small numbers (near 0) if the bridge is keeping up.

### Redis — check position data

```powershell
# List all satellite keys
docker exec redis redis-cli KEYS "sat:*"

# Inspect ISS metadata (NORAD ID 25544)
docker exec redis redis-cli HGETALL sat:meta:25544

# Check flat position data (used by Grafana)
docker exec redis redis-cli EXISTS sat:pos:flat:25544

# Check recent alerts
docker exec redis redis-cli EXISTS alerts:recent
```

### HDFS — check raw position files

```powershell
docker exec namenode bash -c "hdfs dfs -ls /satellite/raw/positions"
```

Files should appear here within a minute of the producers starting.

### Spark — check running applications

```powershell
curl http://localhost:8081/api/v1/applications
```

Or open http://localhost:8081 in a browser. The page should list exactly 2 running applications.

---

## 10. Troubleshooting

### `satellite-api` not reaching healthy status

```powershell
docker logs satellite-api --tail 20
```

If the logs show `No module named ...`, the image is outdated and needs a rebuild:

```powershell
docker compose -f docker\docker-compose.yml build satellite-api
docker compose -f docker\docker-compose.yml up -d satellite-api
```

---

### Spark shows 0 Running Applications

**First, wait.** On the very first run after a clean start, Spark downloads Maven packages. This takes 3–5 minutes. Open http://localhost:8081 and refresh every 30 seconds.

If nothing appears after 5 minutes, run a job interactively to see the actual error:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --total-executor-cores 1 `
  --executor-memory 512m `
  /opt/spark/jobs/streaming/orbit_enrichment.py
```

If the error mentions stale checkpoints or corrupt state, reset:

```powershell
.\scripts\run_streaming.ps1 -Reset
```

---

### API returns empty array `[]`

The speed layer is not running. Diagnose in order:

1. **Check producers** — look at the PowerShell window opened by `run_streaming.ps1`. If it is closed or shows errors, restart: `python -m producers.main`
2. **Check Spark jobs** — open http://localhost:8081. Must show 2 Running Applications.
3. **Check kafka-redis-bridge:**
   ```powershell
   docker logs kafka-redis-bridge --tail 20
   ```

---

### `kafka-redis-bridge` crashes with `UnsupportedCodecError: snappy`

The bridge image predates the `python-snappy` fix. Rebuild it:

```powershell
docker compose -f docker\docker-compose.yml build kafka-redis-bridge
docker compose -f docker\docker-compose.yml up -d kafka-redis-bridge
```

---

### Grafana shows no dashboard (blank page after login)

The Grafana volume is corrupted or was not provisioned correctly. Recreate it:

```powershell
docker compose -f docker\docker-compose.yml stop grafana
docker volume rm docker_grafana_data
docker compose -f docker\docker-compose.yml up -d grafana
```

Wait 30 seconds, then refresh http://localhost:3000.

---

### Grafana panels show "No data"

The dashboard panels read from Redis. Check whether the expected keys exist:

```powershell
docker exec redis redis-cli EXISTS sat:pos:flat:25544
docker exec redis redis-cli EXISTS alerts:recent
```

If neither key exists, the data pipeline is not running. Verify producers and Spark jobs are active (see "API returns empty array" above).

---

### Airflow tasks failing (batch pipelines)

Check the scheduler logs for Python tracebacks:

```powershell
docker logs airflow-scheduler --tail 50
```

You can also view per-task logs directly in the Airflow UI: open the DAG → click a task instance → click **Log**.

---

### Producers crash with `ImportError: attempted relative import`

You ran producers the wrong way. Use the module syntax from the project root:

```powershell
# Wrong — causes ImportError
python producers/main.py

# Correct
python -m producers.main
```

---

### Grafana batch panels show "No data found" after a DAG run

The batch results in Redis may be stale or keyed incorrectly. Clear them and re-trigger the DAG:

```powershell
docker exec redis redis-cli DEL batch:daily:latest batch:daily:list
```

Then in Airflow, re-trigger `satellite_daily_pipeline`. If the panel still shows no data after the run completes, restart the Airflow scheduler to reload updated plugins:

```powershell
docker restart airflow-scheduler
```

---

## 11. Complete Demo Sequence

Follow these steps in order for a full end-to-end demonstration from a cold start.

```powershell
# 1. Start all infrastructure
.\scripts\start.ps1
# Expected: ~3-5 minutes. All containers reach healthy status.

# 2. Start producers and Spark streaming jobs
.\scripts\run_streaming.ps1
# Expected: ~3-5 minutes on first run (Maven download), ~30 seconds on subsequent runs.
# A new PowerShell window opens showing producer logs.
```

After step 2, verify in your browser:

| Check | What to look for |
|---|---|
| http://localhost:3000 | Live ISS position updates within 30 seconds |
| http://localhost:8081 | Exactly 2 Running Applications (SatOrbitEnrichment, SatAnomalyDetection) |
| http://localhost:8080 | Messages accumulating in the `sat.position.enriched` topic |

```powershell
# 3. Trigger the daily batch pipeline in Airflow
# - Open http://localhost:8083
# - Unpause satellite_daily_pipeline
# - Click play → Trigger DAG
# - Wait ~2-3 minutes for completion
# Expected: Grafana "Daily Aggregation" panel shows data

# 4. Trigger the weekly batch pipeline in Airflow
# - Unpause satellite_weekly_pipeline
# - Click play → Trigger DAG
# - Wait ~2-3 minutes for completion
# Expected: Grafana "Weekly TLE Drift" panel shows data
```

At the end of this sequence, all platform layers are active: real-time ingestion, stream processing, batch aggregation, and dashboarding.

---

## 12. Tips and Notes

**Activating the virtual environment**

Before running any `python` or `pip` command outside of Docker, activate the venv:

```powershell
.venv\Scripts\activate
```

Your shell prompt will show `(.venv)` when active. Deactivate with `deactivate`.

**Docker Compose file location**

All `docker compose` commands must include `-f docker\docker-compose.yml`. The compose file is not in the project root.

**Kafka topics are created automatically**

The `kafka-init` one-off container (step 2 of `start.ps1`) creates all required topics on first start. You do not need to create them manually.

**HDFS data persists between restarts**

Unless you run `start.ps1 -Reset` or `docker compose down -v`, all HDFS data, Kafka offsets, and Redis keys survive a `docker compose down` + `docker compose up` cycle.

**Checking container resource usage**

```powershell
docker stats --no-stream
```

If Spark or Airflow containers are using near-100% memory, increase Docker Desktop's RAM allocation.

**Rebuilding a single service after code changes**

```powershell
docker compose -f docker\docker-compose.yml build <service-name>
docker compose -f docker\docker-compose.yml up -d <service-name>
```

Replace `<service-name>` with e.g. `satellite-api`, `kafka-redis-bridge`, etc.

**N2YO API rate limits**

The free N2YO tier allows 1000 transactions per hour. The producer is designed to stay within this limit, but if you run multiple instances simultaneously, you may hit the cap and see empty responses from the N2YO producer.

**NASA DEMO_KEY rate limits**

`DEMO_KEY` is limited to 30 requests per hour per IP. For sustained use, register for a free personal API key at api.nasa.gov.

**Kafka UI username/password**

The Kafka UI at http://localhost:8080 has no authentication configured. Do not expose it to an untrusted network.

**First-time Airflow login**

The Airflow admin user (`admin` / `admin`) is created by the `airflow-init` one-off container during `start.ps1`. If you cannot log in, check: `docker logs airflow-init --tail 20`.
