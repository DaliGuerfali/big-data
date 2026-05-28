"""
Batch Job 1: Daily Orbital Pass Aggregation

Design Pattern: Batch Processing (Lambda Architecture - Batch Layer)

Schedule: Daily at 02:00 UTC via Airflow
Input:  HDFS /satellite/raw/positions/date={date}  (Parquet, from orbit_enrichment streaming job)
Output: HDFS /satellite/aggregated/daily/date={date}/  (ORC format)

Aggregations produced:
  1. satellite_stats  — per-satellite daily summary (distance, altitude, sunlight, countries)
  2. country_stats    — per-satellite per-country overflight counts and time windows
  3. orbit_health     — altitude decay and stability metrics

Usage:
  spark-submit batch/daily_aggregation.py --date 2024-01-15
  # or import and call run_daily_aggregation("2024-01-15") from Airflow
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, avg, min as spark_min, max as spark_max,
    countDistinct, sum as spark_sum, when, first, last,
    to_timestamp, unix_timestamp, lit, stddev, lag,
    abs as spark_abs,
)
from pyspark.sql.window import Window

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:8020")

HDFS_RAW_POSITIONS  = f"{HDFS_NAMENODE}/satellite/raw/positions"
HDFS_AGGREGATED     = f"{HDFS_NAMENODE}/satellite/aggregated/daily"
HDFS_BATCH_TRIGGER  = f"{HDFS_NAMENODE}/satellite/batch_triggers"

BATCH_TRIGGER_TOPIC = "sat.batch.trigger"

# ─── SparkSession ─────────────────────────────────────────────────────────────

def build_spark(app_name: str = "DailyOrbitalAggregation") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.orc.enabled", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-client:3.3.4",
        )
        .getOrCreate()
    )


# ─── Job 1: Per-satellite daily statistics ────────────────────────────────────

def compute_satellite_stats(df: DataFrame) -> DataFrame:
    """
    One row per (satellite_id, satellite_name, orbit_type) per day.

    Columns:
      position_count        — total position samples
      avg/min/max_altitude  — altitude statistics in km
      countries_overflown   — distinct country codes visited
      sunlight_samples      — samples where in_sunlight=true
      eclipse_samples       — samples where in_sunlight=false
      sunlight_pct          — % of day in sunlight
    """
    return (
        df
        .withColumn("orbit_type",  col("orbit.type"))
        .withColumn("in_sunlight", col("lighting.in_sunlight"))
        .withColumn("country_code_flat", col("geo.country_code"))
        .groupBy("satellite_id", "satellite_name", "orbit_type")
        .agg(
            count("*").alias("position_count"),
            avg("altitude_km").alias("avg_altitude_km"),
            spark_min("altitude_km").alias("min_altitude_km"),
            spark_max("altitude_km").alias("max_altitude_km"),
            countDistinct("country_code_flat").alias("countries_overflown"),
            spark_sum(
                when(col("in_sunlight") == True, 1).otherwise(0)
            ).alias("sunlight_samples"),
            spark_sum(
                when(col("in_sunlight") == False, 1).otherwise(0)
            ).alias("eclipse_samples"),
            first("source").alias("primary_source"),
        )
        .withColumn(
            "sunlight_pct",
            when(col("position_count") > 0,
                 (col("sunlight_samples") / col("position_count") * 100.0)
            ).otherwise(lit(0.0))
        )
    )


# ─── Job 2: Country overflight analysis ───────────────────────────────────────

def compute_country_stats(df: DataFrame) -> DataFrame:
    """
    One row per (satellite_id, country_code) per day.

    Columns:
      overflight_samples    — position samples over this country
      first_overflight      — earliest timestamp seen
      last_overflight       — latest timestamp seen
      country_name          — human-readable name
      region                — continent/region
    """
    return (
        df.filter(col("geo.over_ocean") == False)
        .withColumn("country_code", col("geo.country_code"))
        .withColumn("country_name", col("geo.country_name"))
        .withColumn("region",       col("geo.region"))
        .groupBy("satellite_id", "satellite_name", "country_code")
        .agg(
            count("*").alias("overflight_samples"),
            spark_min("timestamp").alias("first_overflight_ts"),
            spark_max("timestamp").alias("last_overflight_ts"),
            first("country_name").alias("country_name"),
            first("region").alias("region"),
        )
    )


# ─── Job 3: Orbital health metrics ────────────────────────────────────────────

def compute_orbit_health(df: DataFrame) -> DataFrame:
    """
    Altitude decay rate and stability metrics per satellite.

    Uses a time-ordered window to compute delta_altitude between consecutive
    samples, then aggregates mean drift rate (km per sample).
    """
    w = Window.partitionBy("satellite_id").orderBy("timestamp")

    with_lag = df.withColumn(
        "prev_altitude",
        lag("altitude_km", 1).over(w)
    ).withColumn(
        "altitude_delta",
        col("altitude_km") - col("prev_altitude")
    ).filter(col("prev_altitude").isNotNull())

    return (
        with_lag
        .groupBy("satellite_id", "satellite_name")
        .agg(
            avg("altitude_delta").alias("mean_altitude_drift_km_per_sample"),
            stddev("altitude_delta").alias("altitude_stability_stddev"),
            spark_sum(
                when(col("altitude_delta") < -0.01, 1).otherwise(0)
            ).alias("descent_samples"),
            spark_sum(
                when(col("altitude_delta") > 0.01, 1).otherwise(0)
            ).alias("ascent_samples"),
        )
    )


# ─── Main entry point ─────────────────────────────────────────────────────────

def run_daily_aggregation(date: str, spark: SparkSession = None) -> None:
    """
    Read one day's enriched position data from HDFS and write three ORC tables.

    Args:
        date: ISO date string, e.g. "2024-01-15"
        spark: optional existing SparkSession (for testing)
    """
    if spark is None:
        spark = build_spark()
        spark.sparkContext.setLogLevel("WARN")

    print(f"[daily-aggregation] Processing date={date}")

    input_path  = f"{HDFS_RAW_POSITIONS}/date={date}"
    output_base = f"{HDFS_AGGREGATED}/date={date}"

    # ── Read ──────────────────────────────────────────────────────────────────
    try:
        df = spark.read.parquet(input_path)
    except Exception as exc:
        print(f"[daily-aggregation] ERROR reading {input_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    row_count = df.count()
    print(f"[daily-aggregation] Loaded {row_count:,} rows from {input_path}")

    if row_count == 0:
        print("[daily-aggregation] No data for this date — nothing to aggregate.")
        return

    df.cache()

    # ── Aggregate ─────────────────────────────────────────────────────────────
    satellite_stats = compute_satellite_stats(df)
    country_stats   = compute_country_stats(df)
    orbit_health    = compute_orbit_health(df)

    # ── Write ORC ─────────────────────────────────────────────────────────────
    (
        satellite_stats
        .write
        .mode("overwrite")
        .format("orc")
        .partitionBy("orbit_type")
        .save(f"{output_base}/satellite_stats")
    )
    print(f"[daily-aggregation] Wrote satellite_stats → {output_base}/satellite_stats")

    (
        country_stats
        .write
        .mode("overwrite")
        .format("orc")
        .save(f"{output_base}/country_stats")
    )
    print(f"[daily-aggregation] Wrote country_stats  → {output_base}/country_stats")

    (
        orbit_health
        .write
        .mode("overwrite")
        .format("orc")
        .save(f"{output_base}/orbit_health")
    )
    print(f"[daily-aggregation] Wrote orbit_health   → {output_base}/orbit_health")

    df.unpersist()

    # ── Cache summary to Redis ────────────────────────────────────────────────
    try:
        import json, os, redis as _redis
        sat_rows   = [r.asDict() for r in satellite_stats.collect()]
        orbit_rows = [r.asDict() for r in orbit_health.collect()]
        summary = {
            "date": date,
            "satellite_stats": sat_rows,
            "country_stats":   [r.asDict() for r in country_stats.collect()],
            "orbit_health":    orbit_rows,
        }
        rc = _redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"))
        rc.setex("batch:daily:latest", 7 * 86400, json.dumps(summary, default=str))

        # Flat summary hash for Grafana (no JSON parsing needed)
        flat = {"date": date, "satellites_tracked": str(len(sat_rows))}
        if sat_rows:
            alts = [float(r.get("avg_altitude_km") or 0) for r in sat_rows if r.get("avg_altitude_km")]
            flat["avg_altitude_km"] = str(round(sum(alts) / len(alts), 1)) if alts else "n/a"
            flat["total_positions"]  = str(sum(int(r.get("total_positions") or 0) for r in sat_rows))
        if orbit_rows:
            for row in orbit_rows:
                otype = str(row.get("orbit_type") or "unknown")
                flat[f"orbit_{otype}_avg_alt"] = str(round(float(row.get("avg_altitude") or 0), 1))
        rc.hset("batch:daily:summary", mapping=flat)
        rc.expire("batch:daily:summary", 7 * 86400)

        # List of per-satellite rows for Grafana LRANGE + extractFields
        rc.delete("batch:daily:list")
        pipe = rc.pipeline()
        for row in sat_rows:
            row["date"] = date
            pipe.rpush("batch:daily:list", json.dumps(row, default=str))
        pipe.expire("batch:daily:list", 7 * 86400)
        pipe.execute()
        rc.close()
        print(f"[daily-aggregation] Cached summary to Redis key batch:daily:latest")
    except Exception as exc:
        print(f"[daily-aggregation] WARNING: Redis cache failed: {exc}", file=sys.stderr)

    print(f"[daily-aggregation] Completed for date={date}")


# ─── Batch trigger publisher ──────────────────────────────────────────────────

def publish_completion_trigger(date: str, spark: SparkSession) -> None:
    """
    Write a completion trigger message to the sat.batch.trigger Kafka topic.
    Called by Airflow after a successful aggregation run.
    """
    import json
    from datetime import datetime, timezone

    msg = {
        "job_type":     "daily_aggregation",
        "date":         date,
        "triggered_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "triggered_by": "batch-job",
        "status":       "completed",
    }

    trigger_df = spark.createDataFrame(
        [(json.dumps(msg),)], ["value"]
    )

    (
        trigger_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", BATCH_TRIGGER_TOPIC)
        .save()
    )
    print(f"[daily-aggregation] Published completion trigger for date={date}")


# ─── CLI entry point ──────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(description="Daily satellite orbital aggregation")
    parser.add_argument(
        "--date",
        default=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Date to process (YYYY-MM-DD). Defaults to yesterday UTC.",
    )
    parser.add_argument(
        "--publish-trigger",
        action="store_true",
        help="Publish a completion message to sat.batch.trigger after success.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    run_daily_aggregation(args.date, spark)

    if args.publish_trigger:
        publish_completion_trigger(args.date, spark)

    spark.stop()
