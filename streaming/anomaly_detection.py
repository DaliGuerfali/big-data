"""
Spark Structured Streaming Job 2: Anomaly Detection & Proximity Alerting

Design Pattern: Stream Processing (Lambda Architecture - Speed Layer)

Input:  sat.position.enriched  (from orbit_enrichment job)
        sat.events.raw         (from DONKIProducer)
Output: sat.alerts  (Kafka)
        Redis       (hot store for serving layer â€” via foreachBatch)

Detection rules:
  1. Velocity anomaly   â€” deviation > 2Ïƒ over 60s sliding window
  2. Altitude anomaly   â€” rapid descent/ascent > 5 km in 60s window
  3. Space weather join â€” satellite in path during active CME/GST event
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import List

import redis as redis_lib

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, udf,
    current_timestamp, window, avg, stddev, min as spark_min,
    max as spark_max, count, abs as spark_abs, expr,
    when, broadcast,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, LongType, TimestampType,
    ArrayType,
)

# â”€â”€â”€ Configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:8020")
REDIS_HOST      = os.getenv("REDIS_HOST", "redis")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))
CHECKPOINT_BASE = f"{HDFS_NAMENODE}/satellite/checkpoints"

INPUT_ENRICHED = "sat.position.enriched"
INPUT_EVENTS   = "sat.events.raw"
OUTPUT_ALERTS  = "sat.alerts"

# Anomaly thresholds
VELOCITY_SIGMA_THRESHOLD  = 2.0   # alert if deviation > 2 standard deviations
ALTITUDE_DROP_KM_PER_60S  = 5.0   # alert if altitude changes > 5 km in 60s
MIN_SAMPLES_FOR_ANOMALY   = 3     # minimum samples before triggering stats alert

# â”€â”€â”€ Input schemas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

GEO_SCHEMA = StructType([
    StructField("country_code",  StringType(),  True),
    StructField("country_name",  StringType(),  True),
    StructField("region",        StringType(),  True),
    StructField("over_ocean",    BooleanType(), True),
])

ORBIT_SCHEMA = StructType([
    StructField("type",           StringType(), True),
    StructField("velocity_km_s",  DoubleType(),  True),
    StructField("period_minutes", DoubleType(),  True),
])

LIGHTING_SCHEMA = StructType([
    StructField("in_sunlight", BooleanType(), True),
])

POSITION_SCHEMA = StructType([
    StructField("latitude",    DoubleType(), True),
    StructField("longitude",   DoubleType(), True),
    StructField("altitude_km", DoubleType(), True),
])

ENRICHED_SCHEMA = StructType([
    StructField("satellite_id",      IntegerType(), True),
    StructField("satellite_name",    StringType(),  True),
    StructField("position",          POSITION_SCHEMA, True),
    StructField("geo",               GEO_SCHEMA,    True),
    StructField("orbit",             ORBIT_SCHEMA,  True),
    StructField("lighting",          LIGHTING_SCHEMA, True),
    StructField("timestamp",         LongType(),    True),
    StructField("source",            StringType(),  True),
    StructField("ingestion_time",    StringType(),  True),
    StructField("processing_time",   StringType(),  True),
])

EVENTS_SCHEMA = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("event_type",     StringType(),  True),
    StructField("start_time",     StringType(),  True),
    StructField("peak_time",      StringType(),  True),
    StructField("end_time",       StringType(),  True),
    StructField("source_location",StringType(),  True),
    StructField("severity",       StringType(),  True),
    StructField("source",         StringType(),  True),
    StructField("ingestion_time", StringType(),  True),
])

# â”€â”€â”€ Alert builder helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _make_alert(alert_type: str, severity: str, satellite_id: int,
                satellite_name: str, details: dict,
                window_start=None, window_end=None) -> dict:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    alert = {
        "alert_id":        str(uuid.uuid4()),
        "alert_type":      alert_type,
        "severity":        severity,
        "satellite_id":    satellite_id,
        "satellite_name":  satellite_name,
        "detected_at":     now,
        "details":         details,
        "source":          "anomaly-detection",
    }
    if window_start and window_end:
        alert["window"] = {"start": str(window_start), "end": str(window_end)}
    return alert


# â”€â”€â”€ Redis writer (used inside foreachBatch) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _write_alerts_to_redis(alerts: List[dict]):
    """Push alert dicts to Redis. Called from foreachBatch worker nodes."""
    if not alerts:
        return
    try:
        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        for alert in alerts:
            alert_id = alert["alert_id"]
            sat_id   = alert["satellite_id"]
            payload  = json.dumps(alert, default=str)

            pipe.setex(f"alert:{alert_id}", 86400, payload)            # 24h TTL
            pipe.lpush(f"sat:alerts:{sat_id}", alert_id)
            pipe.ltrim(f"sat:alerts:{sat_id}", 0, 99)                  # keep 100
        pipe.execute()
        r.close()
    except Exception as exc:
        # Non-fatal: alerts still flow through Kafka
        print(f"[anomaly-detection] Redis write failed: {exc}")


# â”€â”€â”€ foreachBatch sink: Kafka + Redis â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def write_alerts_batch(batch_df: DataFrame, batch_id: int):
    """
    Write each micro-batch of alerts to:
      â€¢ Kafka  sat.alerts  (via DataFrame.write.format("kafka"))
      â€¢ Redis  alert:{id}  (via python-redis)

    Space weather correlation is done here as a batch read rather than a
    stream-stream join, because Spark requires an equality predicate for
    stream-stream joins and there is no natural equality between satellites
    and global solar weather events. Constant lit() keys are folded away
    by Catalyst, so the only correct approach is batch correlation inside
    foreachBatch.
    """
    spark_session = batch_df.sparkSession

    combined_df = batch_df

    # Space weather correlation: batch-read events, cross-join with active satellites
    try:
        events_batch = (
            spark_session.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", INPUT_EVENTS)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(from_json(col("value").cast("string"), EVENTS_SCHEMA).alias("e"))
            .select("e.*")
            .filter(col("severity").isin("high", "extreme"))
            .dropDuplicates(["event_id"])
        )

        if not events_batch.isEmpty() and not batch_df.isEmpty():
            sat_ids = batch_df.select("satellite_id", "satellite_name").distinct()
            sw_alerts = (
                sat_ids.crossJoin(events_batch)
                .select(
                    expr("uuid()").alias("alert_id"),
                    lit("SPACE_WEATHER_CORRELATION").alias("alert_type"),
                    when(col("severity") == "extreme", "CRITICAL")
                    .otherwise("WARNING").alias("severity"),
                    col("satellite_id"),
                    col("satellite_name"),
                    current_timestamp().cast("string").alias("detected_at"),
                    lit("anomaly-detection").alias("source"),
                )
            )
            combined_df = batch_df.union(sw_alerts)
    except Exception as exc:
        print(f"[anomaly-detection] Space weather correlation failed: {exc}")

    if combined_df.isEmpty():
        return

    # Kafka write â€” need value column as JSON string
    kafka_df = combined_df.withColumn(
        "value",
        to_json(struct(*[c for c in combined_df.columns if c != "key"])),
    ).withColumn(
        "key",
        col("satellite_id").cast("string"),
    ).select("key", "value")

    (
        kafka_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", OUTPUT_ALERTS)
        .save()
    )

    # Redis write â€” collect is safe here because alerts are low-volume
    rows = combined_df.collect()
    alerts = [row.asDict(recursive=True) for row in rows]
    _write_alerts_to_redis(alerts)


# â”€â”€â”€ Build SparkSession â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SatAnomalyDetection")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# â”€â”€â”€ Main streaming logic â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # â”€â”€ 1. Read enriched positions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    enriched_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_ENRICHED)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    enriched = (
        enriched_raw
        .select(from_json(col("value").cast("string"), ENRICHED_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", (col("timestamp")).cast(TimestampType()))
        .withWatermark("event_time", "30 seconds")
        # Flatten position subfields for window aggregation
        .withColumn("latitude",    col("position.latitude"))
        .withColumn("longitude",   col("position.longitude"))
        .withColumn("altitude_km", col("position.altitude_km"))
        .withColumn("velocity_km_s", col("orbit.velocity_km_s"))
    )

    # â”€â”€ 2. Sliding window aggregation (60s window, 10s slide) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Note: space weather events are correlated inside write_alerts_batch via
    # a batch Kafka read, not a stream-stream join (which requires an equality
    # predicate that doesn't exist for global solar events).
    windowed = (
        enriched
        .groupBy(
            window("event_time", "60 seconds", "10 seconds"),
            col("satellite_id"),
            col("satellite_name"),
        )
        .agg(
            avg("altitude_km").alias("avg_altitude_km"),
            spark_min("altitude_km").alias("min_altitude_km"),
            spark_max("altitude_km").alias("max_altitude_km"),
            avg("velocity_km_s").alias("avg_velocity_km_s"),
            stddev("velocity_km_s").alias("stddev_velocity_km_s"),
            count("*").alias("sample_count"),
        )
    )

    # â”€â”€ 4. Velocity anomaly UDF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    @udf(returnType=BooleanType())
    def is_velocity_anomaly(avg_v, stddev_v, sample_count):
        if avg_v is None or stddev_v is None or stddev_v == 0:
            return False
        if sample_count is None or sample_count < MIN_SAMPLES_FOR_ANOMALY:
            return False
        # Deviation from expected LEO velocity (~7.66 km/s)
        expected = 7.66
        z_score = abs(avg_v - expected) / stddev_v
        return z_score > VELOCITY_SIGMA_THRESHOLD

    @udf(returnType=BooleanType())
    def is_altitude_anomaly(min_alt, max_alt, sample_count):
        if min_alt is None or max_alt is None:
            return False
        if sample_count is None or sample_count < MIN_SAMPLES_FOR_ANOMALY:
            return False
        return (max_alt - min_alt) > ALTITUDE_DROP_KM_PER_60S

    # â”€â”€ 5. Build alert rows from windowed stats â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    @udf(returnType=StringType())
    def velocity_alert_json(satellite_id, satellite_name, avg_v, stddev_v,
                            win_start, win_end):
        if avg_v is None or stddev_v is None or stddev_v == 0:
            return None
        expected = 7.66
        z_score = abs(avg_v - expected) / stddev_v
        alert = _make_alert(
            alert_type="VELOCITY_ANOMALY",
            severity="WARNING",
            satellite_id=satellite_id,
            satellite_name=satellite_name,
            details={
                "expected_velocity_km_s": expected,
                "actual_velocity_km_s":   round(avg_v, 4),
                "stddev_km_s":            round(stddev_v, 4),
                "deviation_sigma":        round(z_score, 2),
            },
            window_start=win_start,
            window_end=win_end,
        )
        return json.dumps(alert)

    @udf(returnType=StringType())
    def altitude_alert_json(satellite_id, satellite_name, min_alt, max_alt,
                            win_start, win_end):
        delta = (max_alt or 0) - (min_alt or 0)
        alert = _make_alert(
            alert_type="ALTITUDE_ANOMALY",
            severity="WARNING" if delta < 10.0 else "CRITICAL",
            satellite_id=satellite_id,
            satellite_name=satellite_name,
            details={
                "min_altitude_km":     round(min_alt, 2),
                "max_altitude_km":     round(max_alt, 2),
                "delta_km":            round(delta, 2),
                "threshold_km":        ALTITUDE_DROP_KM_PER_60S,
            },
            window_start=win_start,
            window_end=win_end,
        )
        return json.dumps(alert)

    # Filter windows that triggered an anomaly and build alert structs
    vel_anomalies = (
        windowed
        .filter(
            is_velocity_anomaly(
                col("avg_velocity_km_s"),
                col("stddev_velocity_km_s"),
                col("sample_count"),
            )
        )
        .withColumn(
            "alert_json",
            velocity_alert_json(
                col("satellite_id"), col("satellite_name"),
                col("avg_velocity_km_s"), col("stddev_velocity_km_s"),
                col("window.start"), col("window.end"),
            ),
        )
    )

    alt_anomalies = (
        windowed
        .filter(
            is_altitude_anomaly(
                col("min_altitude_km"),
                col("max_altitude_km"),
                col("sample_count"),
            )
        )
        .withColumn(
            "alert_json",
            altitude_alert_json(
                col("satellite_id"), col("satellite_name"),
                col("min_altitude_km"), col("max_altitude_km"),
                col("window.start"), col("window.end"),
            ),
        )
    )

    # Parse the JSON alert back into columns for the output schema
    ALERT_SCHEMA = StructType([
        StructField("alert_id",       StringType(),  True),
        StructField("alert_type",     StringType(),  True),
        StructField("severity",       StringType(),  True),
        StructField("satellite_id",   IntegerType(), True),
        StructField("satellite_name", StringType(),  True),
        StructField("detected_at",    StringType(),  True),
        StructField("source",         StringType(),  True),
    ])

    def parse_alert_df(df):
        return (
            df
            .select(from_json(col("alert_json"), ALERT_SCHEMA).alias("a"))
            .select("a.*")
            .filter(col("alert_id").isNotNull())
        )

    alerts_df = parse_alert_df(vel_anomalies).union(parse_alert_df(alt_anomalies))

    # â”€â”€ 6. Space weather correlation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Handled inside write_alerts_batch via batch Kafka read + crossJoin.
    # Stream-stream join is not viable here: Catalyst constant-folds any
    # lit("x") == lit("x") equality predicate away, leaving only the range
    # condition which Spark rejects at plan time.

    # â”€â”€ 7. Write â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    all_alerts = alerts_df

    query = (
        all_alerts
        .writeStream
        .foreachBatch(write_alerts_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/anomaly-alerts")
        .trigger(processingTime="20 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
