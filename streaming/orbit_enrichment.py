"""
Spark Structured Streaming Job 1: Real-time Orbit Normalization & Geo-enrichment

Design Pattern: Stream Processing (Lambda Architecture - Speed Layer)

Input:  sat.position.raw   (messages from ISSProducer + N2YOProducer)
Output: sat.position.enriched (Kafka)
        hdfs:///satellite/raw/positions (Parquet, partitioned by date/satellite_id)

Transformations:
  1. Schema normalization — unify Open-Notify (no altitude) and N2YO (full fields)
  2. Geo-enrichment      — reverse geocode lat/lon → country_code, country_name, region, over_ocean
  3. Orbit classification — altitude_km → orbit type (LEO/MEO/GEO/HEO) + velocity + period
  4. Lighting indicator  — simplified solar angle → in_sunlight flag
"""

import json
import os
import math
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, udf,
    current_timestamp, to_date, when,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, LongType, TimestampType,
)

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:8020")
CHECKPOINT_BASE = f"{HDFS_NAMENODE}/satellite/checkpoints"

INPUT_TOPIC    = "sat.position.raw"
OUTPUT_TOPIC   = "sat.position.enriched"
HDFS_OUTPUT    = f"{HDFS_NAMENODE}/satellite/raw/positions"

# ─── Input schema (union of Open-Notify + N2YO fields) ───────────────────────

RAW_SCHEMA = StructType([
    StructField("satellite_id",   IntegerType(), True),
    StructField("satellite_name", StringType(),  True),
    StructField("latitude",       DoubleType(),  True),
    StructField("longitude",      DoubleType(),  True),
    StructField("altitude_km",    DoubleType(),  True),   # null for Open-Notify
    StructField("azimuth",        DoubleType(),  True),
    StructField("elevation",      DoubleType(),  True),
    StructField("ra",             DoubleType(),  True),
    StructField("dec",            DoubleType(),  True),
    StructField("timestamp",      LongType(),    True),   # Unix epoch
    StructField("source",         StringType(),  True),
    StructField("ingestion_time", StringType(),  True),
])

# ─── Geo-enrichment: offline bounding-box lookup ─────────────────────────────
#
# We use a simplified static table of continental bounding boxes.
# In production you would use Natural Earth GeoJSON + shapely/geopandas.
# For this project, a two-tier lookup (continent → country rough box) is enough
# to demonstrate the enrichment pattern without adding binary dependencies.

_GEO_TABLE = [
    # (lat_min, lat_max, lon_min, lon_max, country_code, country_name, region)
    (  49.0,  60.0, -140.0,  -60.0, "CA", "Canada",            "North America"),
    (  24.0,  49.0, -125.0,  -66.0, "US", "United States",     "North America"),
    (  14.0,  32.0,  -92.0,  -86.0, "MX", "Mexico",            "North America"),
    (  36.0,  71.0,  -10.0,   40.0, "EU", "Europe",            "Europe"),
    (  36.0,  47.0,   26.0,   45.0, "TR", "Turkey",            "Europe"),
    (  -5.0,  37.0,  -17.0,   51.0, "AF", "Africa",            "Africa"),
    (  -35.0,-22.0,  -70.0,  -35.0, "AR", "Argentina",         "South America"),
    (  -23.0,  5.0,  -74.0,  -34.0, "BR", "Brazil",            "South America"),
    (  -55.0,-22.0,  -75.0,  -53.0, "SA", "South America",     "South America"),
    (   8.0,  37.0,   68.0,   97.0, "IN", "India",             "Asia"),
    (  18.0,  53.0,   73.0,  135.0, "CN", "China",             "Asia"),
    (  30.0,  46.0,   26.0,   77.0, "ME", "Middle East",       "Asia"),
    (  51.0,  77.0,   37.0,  180.0, "RU", "Russia",            "Europe"),
    ( -45.0, -10.0,  110.0,  155.0, "AU", "Australia",         "Oceania"),
    (  30.0,  45.0,  129.0,  145.0, "JP", "Japan",             "Asia"),
    (  33.0,  38.0,  124.0,  131.0, "KR", "South Korea",       "Asia"),
    ( -55.0, -20.0,  -80.0,  -65.0, "CL", "Chile",             "South America"),
]


def _geo_lookup(lat: float, lon: float):
    """Return (country_code, country_name, region, over_ocean) for a coordinate."""
    if lat is None or lon is None:
        return ("??", "Unknown", "Unknown", True)
    for lat_min, lat_max, lon_min, lon_max, code, name, region in _GEO_TABLE:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return (code, name, region, False)
    return ("--", "Ocean / Unclaimed", "Ocean", True)


# ─── Orbit classification ─────────────────────────────────────────────────────

# ISS altitude from Open-Notify has no altitude; use 408 km as default for LEO.
ISS_DEFAULT_ALTITUDE_KM = 408.0
EARTH_RADIUS_KM = 6371.0
GM = 3.986004418e14   # Earth's gravitational parameter (m³/s²)


def _classify_orbit(altitude_km: float):
    """Return (orbit_type, velocity_km_s, period_minutes) from altitude."""
    if altitude_km is None or altitude_km <= 0:
        altitude_km = ISS_DEFAULT_ALTITUDE_KM

    r = (EARTH_RADIUS_KM + altitude_km) * 1000   # metres
    v_ms = math.sqrt(GM / r)                       # circular orbit velocity
    v_kms = v_ms / 1000.0
    period_s = 2 * math.pi * r / v_ms
    period_min = period_s / 60.0

    if altitude_km < 2000:
        orbit_type = "LEO"
    elif altitude_km < 35786:
        orbit_type = "MEO"
    elif 35786 <= altitude_km <= 35800:
        orbit_type = "GEO"
    else:
        orbit_type = "HEO"

    return (orbit_type, round(v_kms, 3), round(period_min, 2))


# ─── Lighting indicator ───────────────────────────────────────────────────────

def _in_sunlight(lat: float, lon: float, unix_ts: int) -> bool:
    """
    Simplified solar elevation estimate.

    Uses the equation of time approximation and the declination formula to
    decide whether the sub-satellite point is in sunlight. Good enough for
    a Big Data demo; replace with skyfield for precision.
    """
    if lat is None or lon is None or unix_ts is None:
        return True

    dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    day_of_year = dt.timetuple().tm_yday

    # Solar declination (degrees)
    decl = 23.45 * math.sin(math.radians((360 / 365) * (day_of_year - 81)))

    # Hour angle
    utc_hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    solar_noon_lon = -15.0 * (utc_hour - 12)
    hour_angle = lon - solar_noon_lon

    # Solar elevation
    sin_elev = (
        math.sin(math.radians(lat)) * math.sin(math.radians(decl))
        + math.cos(math.radians(lat)) * math.cos(math.radians(decl))
        * math.cos(math.radians(hour_angle))
    )
    elevation_deg = math.degrees(math.asin(max(-1, min(1, sin_elev))))
    return elevation_deg > -6.0   # civil twilight threshold


# ─── Register UDFs ────────────────────────────────────────────────────────────

GEO_RESULT_TYPE = StructType([
    StructField("country_code",  StringType(), True),
    StructField("country_name",  StringType(), True),
    StructField("region",        StringType(), True),
    StructField("over_ocean",    BooleanType(), True),
])

ORBIT_RESULT_TYPE = StructType([
    StructField("type",           StringType(), True),
    StructField("velocity_km_s",  DoubleType(),  True),
    StructField("period_minutes", DoubleType(),  True),
])


@udf(returnType=GEO_RESULT_TYPE)
def geo_enrich_udf(lat, lon):
    code, name, region, ocean = _geo_lookup(lat, lon)
    return (code, name, region, ocean)


@udf(returnType=ORBIT_RESULT_TYPE)
def orbit_classify_udf(altitude_km):
    otype, vel, period = _classify_orbit(altitude_km)
    return (otype, vel, period)


@udf(returnType=BooleanType())
def sunlight_udf(lat, lon, unix_ts):
    return _in_sunlight(lat, lon, unix_ts)


# ─── Build SparkSession ───────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SatOrbitEnrichment")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-client:3.3.4",
        )
        .getOrCreate()
    )


# ─── Main streaming logic ─────────────────────────────────────────────────────

def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read raw positions from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload, cast unix epoch to TimestampType, then watermark
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"))
        .select("d.*")
    )

    parsed = parsed.withColumn(
        "event_time",
        col("timestamp").cast(TimestampType()),
    ).withWatermark("event_time", "30 seconds")

    # Fill missing altitude for Open-Notify source
    parsed = parsed.withColumn(
        "altitude_km",
        when(col("altitude_km").isNull(), lit(ISS_DEFAULT_ALTITUDE_KM))
        .otherwise(col("altitude_km")),
    )

    # 3. Apply enrichment UDFs
    geo    = geo_enrich_udf(col("latitude"), col("longitude"))
    orbit  = orbit_classify_udf(col("altitude_km"))
    sunlit = sunlight_udf(col("latitude"), col("longitude"), col("timestamp"))

    enriched = (
        parsed
        .withColumn("geo",   geo)
        .withColumn("orbit", orbit)
        .withColumn(
            "lighting",
            struct(
                sunlit.alias("in_sunlight"),
            ),
        )
        .withColumn("processing_time", current_timestamp())
        .withColumn("date", to_date(col("event_time")))
    )

    # 4. Build the output struct matching the enriched schema
    output_struct = struct(
        col("satellite_id"),
        col("satellite_name"),
        struct(
            col("latitude"),
            col("longitude"),
            col("altitude_km"),
        ).alias("position"),
        col("geo"),
        col("orbit"),
        col("lighting"),
        col("timestamp"),
        col("source"),
        col("ingestion_time"),
        col("processing_time").cast("string").alias("processing_time"),
    )

    enriched_out = enriched.withColumn("value", to_json(output_struct))

    # 5. Single foreachBatch sink: write to Kafka + HDFS in one query.
    #    Using two separate writeStream on the same source causes the second
    #    query's start() to block waiting on the first's checkpoint commit.

    _batch_count = [0]

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # ── Kafka ──────────────────────────────────────────────────────
        (
            batch_df
            .select(
                col("satellite_id").cast("string").alias("key"),
                col("value"),
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", OUTPUT_TOPIC)
            .save()
        )

        # ── HDFS Parquet (every ~12 batches ≈ 1 minute at 5s trigger) ─
        _batch_count[0] += 1
        if _batch_count[0] % 12 == 0:
            (
                batch_df
                .select(
                    col("satellite_id"),
                    col("satellite_name"),
                    col("latitude"),
                    col("longitude"),
                    col("altitude_km"),
                    col("geo"),
                    col("orbit"),
                    col("lighting"),
                    col("timestamp"),
                    col("source"),
                    col("ingestion_time"),
                    col("processing_time"),
                    col("date"),
                )
                .write
                .format("parquet")
                .mode("append")
                .partitionBy("date", "satellite_id")
                .save(HDFS_OUTPUT)
            )

    query = (
        enriched_out
        .writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/enrichment")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
