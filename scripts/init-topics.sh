#!/bin/bash
# ─────────────────────────────────────────────────────────
# Kafka Topic Initialization Script
# Creates all topics required by the satellite tracking platform
# ─────────────────────────────────────────────────────────

set -e

KAFKA_BROKER="kafka:9092"
REPLICATION=1  # 1 for single-broker dev; increase for production

echo "======================================================"
echo " Kafka Topic Initialization"
echo "======================================================"
echo " Broker: $KAFKA_BROKER"
echo ""

# Helper function
create_topic() {
    local TOPIC=$1
    local PARTITIONS=$2
    local RETENTION_MS=$3
    local DESCRIPTION=$4

    echo "Creating topic: $TOPIC"
    echo "  Partitions : $PARTITIONS"
    echo "  Retention  : $(( RETENTION_MS / 3600000 ))h"
    echo "  Description: $DESCRIPTION"

    kafka-topics --bootstrap-server "$KAFKA_BROKER" \
        --create \
        --if-not-exists \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --config "retention.ms=$RETENTION_MS" \
        --config "cleanup.policy=delete"

    echo "  ✓ Done"
    echo ""
}

# ── Wait for Kafka to be ready ───────────────────────────
echo "Waiting for Kafka broker to be ready..."
until kafka-broker-api-versions --bootstrap-server "$KAFKA_BROKER" > /dev/null 2>&1; do
    echo "  Kafka not ready yet, retrying in 3s..."
    sleep 3
done
echo "✓ Kafka is ready!"
echo ""

# ── Create Topics ────────────────────────────────────────

# Raw satellite position data (ISS + N2YO)
# High throughput: 6 partitions, short retention (1h)
create_topic "sat.position.raw" 6 3600000 \
    "Raw lat/lon position from ISS and N2YO producers"

# Raw TLE orbital element sets (bulk fetcher, hourly)
# Low throughput: 3 partitions, 24h retention for reprocessing
create_topic "sat.tle.raw" 3 86400000 \
    "Raw TLE records from TLE API bulk fetcher"

# Raw space weather events from NASA DONKI
# Low throughput: 3 partitions, 72h retention (events may be updated)
create_topic "sat.events.raw" 3 259200000 \
    "Space weather events: CME, GST, FLR, SEP, IPS, HSS"

# Enriched positions (output of Spark Structured Streaming Op1)
# Same parallelism as input: 6 partitions, 6h retention
create_topic "sat.position.enriched" 6 21600000 \
    "Geo-enriched positions with country/region/orbit metadata"

# Anomaly alerts (output of Spark Structured Streaming Op2)
# Low throughput: 2 partitions, 24h retention
create_topic "sat.alerts" 2 86400000 \
    "Anomaly and proximity alerts from sliding-window detection"

# Batch job trigger messages (published by Airflow)
# Single partition, 7 day retention for audit trail
create_topic "sat.batch.trigger" 1 604800000 \
    "Batch pipeline trigger events from Airflow scheduler"

# ── Verify all topics created ────────────────────────────
echo "======================================================"
echo " Topic Verification"
echo "======================================================"
kafka-topics --bootstrap-server "$KAFKA_BROKER" --list | grep "^sat\." | sort

echo ""
echo "======================================================"
echo " All topics initialized successfully!"
echo "======================================================"
