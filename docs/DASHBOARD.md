# Grafana Dashboard — Satellite Tracking Big Data Platform

## Overview

This Grafana dashboard provides a unified view of the satellite tracking Lambda Architecture platform. It visualises two concurrent data layers:

- **Speed layer** — real-time position, anomaly alerts, and space weather events processed through Kafka and Spark Structured Streaming, arriving in Redis within seconds and displayed here with a 10-second auto-refresh.
- **Batch layer** — daily and weekly aggregations produced by Airflow-orchestrated Spark and MapReduce jobs, stored in Redis as pre-computed summaries.

The dashboard is the observation surface for a pipeline that tracks three satellites (ISS, Hubble, NOAA-20), ingests NASA space weather data, and runs automated anomaly detection in real time.

**Access:** `http://localhost:3000` — credentials `admin / admin`  
**Auto-refresh:** every 10 seconds (configured in the top-right Grafana refresh picker)

---

## Section 1 — Live Satellite Tracking

> Row header: **Live Satellite Tracking**

This section confirms that the three tracked satellites are visible in Redis and shows the current contextual state of the ISS.

### Satellite Name Panels (3 table panels)

Each panel performs a Redis `HGET sat:meta:{id} name` command and displays the result as a single-row table.

| Panel | NORAD ID | Expected value |
|---|---|---|
| ISS | 25544 | `International Space Station` |
| Hubble | 20580 | `Hubble Space Telescope` |
| NOAA-20 | 43013 | `NOAA-20` |

If a panel shows **No data**, the satellite metadata has not been loaded into Redis. Run the producer or the metadata seed script to populate `sat:meta:{id}`.

### Contextual State Panels

**ISS Orbit Class** — `HGET sat:pos:flat:25544 orbit_type`

Possible values: `LEO` (Low Earth Orbit, below ~2 000 km), `MEO` (Medium Earth Orbit), `GEO` (Geostationary), `HEO` (Highly Elliptical). The ISS always returns `LEO`.

**ISS Sunlight** — `HGET sat:pos:flat:25544 in_sunlight`

Colour-coded stat panel:
- **Yellow** — `true`: the ISS is in direct sunlight
- **Dark blue** — `false`: the ISS is in Earth's shadow (eclipse)

The ISS alternates roughly every 45 minutes between sunlight and shadow as it completes each ~92-minute orbit.

**ISS Over** — `HGET sat:pos:flat:25544 country`

Displays the country or ocean region the ISS is currently flying over, derived during Spark enrichment from a reverse-geocoding step applied to the raw latitude/longitude. Typical values: `Pacific Ocean`, `Russia`, `United States`, `Atlantic Ocean`, etc.

**Redis Keys** — `DBSIZE`

Total number of keys currently held in Redis. A healthy running system typically shows several hundred to a few thousand keys depending on how long the pipeline has been running and the TTL configuration. Displayed as a blue stat panel.

**Active Weather Events** — `SCARD events:active`

Count of currently active solar events stored in the `events:active` Redis set. Colour thresholds:
- **Green** — 0 active events: nominal
- **Orange** — 1–3 active events: moderate solar activity
- **Red** — 4+ active events: elevated solar activity; anomaly correlation alerts are likely

---

## Section 2 — ISS Live Position

> Row header: **ISS Live Position — updates every ~15s via Kafka -> Spark -> Redis**

Four stat panels reading from the Redis hash `sat:pos:flat:25544`. This hash is written by the Kafka-Redis bridge each time the Spark Structured Streaming job processes a new enriched position message.

| Panel | Field | Expected range | Notes |
|---|---|---|---|
| Latitude (deg) | `latitude` | −51.6° to +51.6° | Constrained by the ISS orbital inclination of 51.6° |
| Longitude (deg) | `longitude` | −180° to +180° | Wraps at the antimeridian |
| Altitude (km) | `altitude` | 400–420 km | Nominal ISS operational altitude; occasional boosts move it closer to 420 km |
| Velocity (km/s) | `velocity` | ~7.66 km/s | Equivalent to approximately 27 600 km/h |

**What to watch for during a demo:**

- Latitude should oscillate smoothly between the inclination limits over roughly 46 minutes (half-orbit) as you observe across multiple refreshes.
- Longitude advances approximately 2.5° per minute eastward during normal orbit.
- Altitude deviations greater than ±5 km from the nominal range within a short window trigger an `ALTITUDE_ANOMALY` alert visible in Section 3.
- Velocity deviations greater than 2 standard deviations from the rolling mean trigger a `VELOCITY_ANOMALY` alert.

These panels update whenever a new Kafka message completes the full pipeline: Producer → Kafka → Spark → Kafka → kafka-redis-bridge → Redis. End-to-end latency is typically 10–20 seconds.

---

## Section 3 — Anomaly Detection

> Row header: **Anomaly Detection — Speed Layer: Spark Structured Streaming -> Redis**

### Recent Anomaly Alerts (table panel)

**Redis command:** `LRANGE alerts:recent 0 14`  
Displays the 15 most recent anomaly alerts, newest first.

| Column | Description |
|---|---|
| Alert Type | The category of anomaly detected |
| Severity | `LOW`, `MEDIUM`, or `HIGH` |
| Satellite | Human-readable satellite name |
| NORAD ID | Numeric identifier (e.g. `25544`) |
| Detected At | ISO-8601 timestamp when the anomaly was identified |
| Source | Pipeline stage that produced the alert (e.g. `spark-streaming`) |

**Alert types explained:**

- **VELOCITY_ANOMALY** — The instantaneous velocity deviated more than 2 sigma from the expected orbital velocity. This can indicate a propulsion burn (ISS periodically re-boosts altitude), sensor noise in the TLE-derived position, or a genuine anomaly.
- **ALTITUDE_ANOMALY** — Altitude changed by more than 5 km within a 60-second sliding window. Expected during planned re-boost manoeuvres; unexpected changes may indicate atmospheric drag anomalies at solar maximum.
- **SPACE_WEATHER_CORRELATION** — The satellite's current position falls within the active region associated with a solar event (CME, HSS, or flare) recorded in `events:active`. This is a correlation alert, not a confirmed impact.

If the table is empty, either no anomalies have been detected in the current session or the `alerts:recent` list has not been seeded. Verify the Spark streaming job is running and processing messages from the enriched Kafka topic.

---

## Section 4 — Space Weather Events

> Row header: **Space Weather Events — NASA DONKI producer -> Kafka -> Redis**

### Recent Space Weather Events (table panel)

**Redis command:** `LRANGE events:recent 0 14`  
Displays the 15 most recently ingested NASA DONKI space weather events.

| Column | Description |
|---|---|
| Type | CME, HSS, or FLR (see below) |
| Start Time | Event start in ISO-8601 UTC |
| End Time | Event end time (may be `null` for ongoing events) |
| Speed (km/s) | CME propagation speed; blank for flares |
| Source Location | Heliographic coordinates (e.g. `N15W20`) |
| Instruments | Observing instruments (e.g. `SOHO/LASCO C2`) |
| Ingested At | Timestamp when the event entered the pipeline |

**Event types explained:**

- **CME (Coronal Mass Ejection)** — A large expulsion of plasma and magnetic field from the Sun's corona. High-speed CMEs (>800 km/s) can reach Earth in 1–3 days and cause geomagnetic storms. The `Speed (km/s)` column shows the CME's initial propagation speed.
- **HSS (High Speed Stream)** — A fast-moving region of solar wind from a coronal hole. Less violent than CMEs but can cause sustained geomagnetic activity. Speed is measured at the L1 monitoring point.
- **FLR (Solar Flare)** — An intense burst of radiation. Classified by X-ray peak flux: A, B, C, M, or X class. X-class flares can cause radio blackouts and increase radiation exposure for satellites in LEO.

Events visible here are the raw feed from the NASA DONKI API producer. The anomaly detection engine correlates these events with satellite positions in real time; a correlation appears in Section 3 as a `SPACE_WEATHER_CORRELATION` alert.

---

## Section 5 — System Health

> Row header: **System Health**

A quick-glance summary confirming the pipeline is alive and data is flowing.

| Panel | Redis command | What it tells you |
|---|---|---|
| Redis Keys (total) | `DBSIZE` | Total keys in Redis; should grow as the pipeline runs |
| Active Weather Events | `SCARD events:active` | Number of solar events currently flagged as active |
| ISS Last Seen | `HGET sat:meta:25544 last_seen` | Timestamp of the most recent processed ISS position; converted from Unix epoch to a human-readable UTC time |

**Interpreting ISS Last Seen:**

- If the timestamp is within the last 30 seconds: the speed layer pipeline is healthy.
- If the timestamp is 30–120 seconds old: there may be a brief Kafka consumer lag or a Spark micro-batch delay. Usually self-resolving.
- If the timestamp is more than 2 minutes old: investigate the Kafka broker, the Spark streaming job, or the kafka-redis-bridge. See the Troubleshooting section below.

---

## Section 6 — Batch Analytics

> Row header: **Batch Analytics — Batch Layer: Airflow + Spark + MapReduce -> Redis**

This section shows the output of scheduled batch jobs. Unlike the speed layer, these panels do not update every 10 seconds — they reflect the most recent completed batch run.

### Daily Aggregation (table panel)

**Redis command:** `LRANGE batch:daily:list 0 -1`

Shows per-satellite daily summary statistics computed by the Spark batch job.

| Column | Description |
|---|---|
| NORAD ID | Satellite identifier |
| Satellite | Human-readable name |
| Avg Alt (km) | Mean altitude over all positions recorded that day |
| Positions | Number of position records processed |
| Orbit | Orbit classification (LEO/MEO/GEO/HEO) |
| Date | The calendar date this summary covers (UTC) |
| Countries | Comma-separated list of countries/regions overflown |
| Sunlight % | Percentage of the day spent in sunlight (ISS: typically ~60%) |

### Weekly TLE Drift (table panel)

**Redis command:** `LRANGE batch:weekly:list 0 -1`

Shows per-satellite TLE (Two-Line Element) drift analysis produced by the MapReduce job, comparing TLE snapshots across the week.

| Column | Description |
|---|---|
| NORAD ID | Satellite identifier |
| Anomaly? | `true` if TLE drift exceeded the threshold; `false` otherwise |
| TLE Snapshots | Number of TLE snapshots compared during the analysis window |
| Period | Orbital period in minutes derived from the latest TLE |

**How to refresh batch panels:**

Batch data is written by Airflow DAGs on a schedule (typically daily and weekly). To trigger a batch run manually during a demo:

1. Open the Airflow UI (default: `http://localhost:8080`).
2. Locate the DAG named `satellite_daily_aggregation` or `satellite_weekly_tle_drift`.
3. Click **Trigger DAG** (the play button).
4. Wait for the DAG run to complete (green status).
5. Return to the Grafana dashboard — the batch panels will reflect the new data on the next auto-refresh cycle.

Alternatively, from the command line:
```bash
# Trigger the daily batch job
airflow dags trigger satellite_daily_aggregation

# Trigger the weekly TLE drift job
airflow dags trigger satellite_weekly_tle_drift
```

If batch panels show **No data**, the batch jobs have not yet run in the current environment. Trigger the DAGs manually as described above.

---

## Data Flow Architecture

```
                          SPEED LAYER (seconds latency)
                          ─────────────────────────────
  TLE / NASA DONKI API
        │
        ▼
  Kafka Producers ──────► Kafka Topics (raw)
                                │
                                ▼
                    Spark Structured Streaming
                    (enrichment: geocoding,
                     anomaly detection,
                     weather correlation)
                                │
                                ▼
                    Kafka Topics (enriched)
                                │
                                ▼
                    kafka-redis-bridge
                                │
                                ▼
                            Redis ──────────────► Grafana
                                                 (10s refresh)


                          BATCH LAYER (hours latency)
                          ───────────────────────────
  HDFS (historical
  position archive)
        │
        ▼
  Airflow DAG Scheduler
        │
        ├──► Spark Batch Job (daily aggregation)
        │           │
        └──► MapReduce Job (weekly TLE drift)
                    │
                    ▼
                Redis ──────────────────────────► Grafana
                                                 (on next refresh
                                                  after job completes)
```

**Lambda Architecture summary:**

The platform implements the Lambda Architecture pattern:

- **Speed layer** answers "what is happening right now?" — position data is seconds old, anomaly alerts fire within one processing window (~15s), and weather events appear within minutes of NASA publishing them.
- **Batch layer** answers "what happened over the complete dataset?" — daily and weekly aggregations run against the full HDFS archive, ensuring accuracy and completeness that the speed layer's approximations cannot guarantee.
- **Grafana** is the serving layer — it merges both views into a single dashboard, letting operators see current state alongside historical context simultaneously.

---

## Interpreting the Data

### Normal operating state

| Indicator | Expected value |
|---|---|
| ISS Altitude | 400–420 km |
| ISS Velocity | 7.65–7.67 km/s |
| ISS Latitude | Oscillating between −51.6° and +51.6° |
| Redis Keys | Growing slowly over time |
| Active Weather Events | 0–2 (solar minimum), up to 10+ during solar maximum |
| ISS Last Seen | Within the last 30 seconds |

### What anomalies look like

A `VELOCITY_ANOMALY` alert during a demo is most likely caused by a planned orbital re-boost event injected by the simulator, or by a TLE epoch crossing where the propagated position jumps. These are expected and demonstrate that the detection pipeline is functioning.

An `ALTITUDE_ANOMALY` alert within a demo scenario confirms the 60-second sliding window logic in Spark is operating correctly.

A `SPACE_WEATHER_CORRELATION` alert confirms end-to-end integration: the NASA DONKI producer emitted an event, it traversed Kafka and was stored in Redis, and the Spark streaming job successfully correlated the satellite's current position with the event's active region.

### Batch vs. speed layer consistency

The daily aggregation average altitude should be close to the live altitude shown in Section 2. Small differences (1–3 km) are normal due to atmospheric drag variations and the difference between instantaneous readings and time-averaged values. Large differences (>10 km) indicate the batch job ran against a different date's data or there is a time zone mismatch in the aggregation logic.

---

## Troubleshooting — Common "No Data" Situations

### All panels show "No data"

The Grafana Redis plugin is not connected to Redis. Check:
1. Redis is running: `docker ps | grep redis` or `redis-cli ping` (should return `PONG`).
2. The Grafana Redis datasource is configured at `http://localhost:3000/connections/datasources`. Verify the host is `redis:6379` (Docker network) or `localhost:6379` (host network) depending on your deployment.
3. Click **Save & Test** on the datasource page to confirm connectivity.

### ISS position panels show "No data"

The key `sat:pos:flat:25544` does not exist in Redis.
- Verify the Kafka producer is running and emitting messages to the raw positions topic.
- Verify the Spark Structured Streaming job is running: check the Spark UI (default `http://localhost:4040`) for active streaming queries.
- Verify the kafka-redis-bridge is running and consuming from the enriched topic.
- Manually check: `redis-cli HGETALL sat:pos:flat:25544`

### Satellite name panels show "No data"

The metadata hash `sat:meta:{id}` has not been populated.
- Run the metadata seed script (typically `python seed_metadata.py` or equivalent in the producers directory).
- Manually check: `redis-cli HGETALL sat:meta:25544`

### Anomaly alerts table is empty

Either no anomalies have been detected yet, or the alerts list was cleared.
- To confirm the pipeline is healthy, check that ISS position is updating in Section 2.
- To trigger a demo anomaly, inject an out-of-range position via the simulator.
- Manually check: `redis-cli LRANGE alerts:recent 0 14`

### Space weather events table is empty

The NASA DONKI producer has not run or the API returned no events for the configured time window.
- Restart the DONKI producer service.
- Check producer logs for HTTP errors from the NASA API (rate limiting, network issues).
- Manually check: `redis-cli LRANGE events:recent 0 14`

### Batch panels are empty

The batch jobs have not yet been triggered.
- Trigger manually via Airflow UI or CLI (see the "How to refresh batch panels" section above).
- Manually check: `redis-cli LRANGE batch:daily:list 0 -1`

### ISS Last Seen shows an old timestamp

The speed layer has stalled. Check in order:
1. `redis-cli HGET sat:meta:25544 last_seen` — if this is recent, the Grafana query expression may need adjustment.
2. Kafka consumer lag: check the Kafka UI or run `kafka-consumer-groups.sh --describe` for the Spark consumer group.
3. Spark streaming job health: `http://localhost:4040` → Streaming tab → check for batch processing delays or failed batches.
4. kafka-redis-bridge logs for connection errors.

---

## Quick Reference — Redis Keys Used by the Dashboard

| Redis key | Type | Contents |
|---|---|---|
| `sat:meta:25544` | Hash | ISS metadata (name, last_seen, etc.) |
| `sat:meta:20580` | Hash | Hubble metadata |
| `sat:meta:43013` | Hash | NOAA-20 metadata |
| `sat:pos:flat:25544` | Hash | ISS latest position (lat, lon, alt, vel, orbit_type, in_sunlight, country) |
| `alerts:recent` | List | Last 15 anomaly alerts (newest first) |
| `events:recent` | List | Last 15 space weather events (newest first) |
| `events:active` | Set | IDs of currently active space weather events |
| `batch:daily:list` | List | Daily aggregation results per satellite |
| `batch:weekly:list` | List | Weekly TLE drift analysis results per satellite |
