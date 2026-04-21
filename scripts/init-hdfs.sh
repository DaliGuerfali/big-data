#!/bin/bash
# ─────────────────────────────────────────────────────────
# HDFS Directory Initialization Script
# Creates the full directory tree for the satellite platform
# ─────────────────────────────────────────────────────────

set -e

echo "======================================================"
echo " HDFS Directory Initialization"
echo "======================================================"

# Wait for HDFS to be out of safe mode
echo "Waiting for HDFS to exit safe mode..."
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
    echo "  HDFS in safe mode, waiting 5s..."
    sleep 5
done
echo "✓ HDFS ready!"
echo ""

# Helper function
mkdirs() {
    local DIR=$1
    local DESCRIPTION=$2
    echo "Creating: $DIR"
    echo "  Purpose: $DESCRIPTION"
    hdfs dfs -mkdir -p "$DIR"
    echo "  ✓ Done"
}

# ── Raw Data Zone ─────────────────────────────────────────
# Parquet files partitioned by date and satellite_id
mkdirs "/satellite/raw/positions" \
    "Enriched Parquet position records — partitioned by date=YYYY-MM-DD/satellite_id=N"

# TLE records partitioned by date
mkdirs "/satellite/raw/tle" \
    "Raw TLE JSON records — partitioned by date=YYYY-MM-DD"

# Space weather event records partitioned by date and type
mkdirs "/satellite/raw/events" \
    "NASA DONKI events — partitioned by date=YYYY-MM-DD/event_type=CME|FLR|..."

# ── Aggregated Data Zone ──────────────────────────────────
# ORC format — daily orbital statistics (Spark batch job)
mkdirs "/satellite/aggregated/daily" \
    "Daily orbital summaries in ORC — partitioned by date/orbit_type"

# ORC format — weekly summaries (Spark batch job)
mkdirs "/satellite/aggregated/weekly" \
    "Weekly aggregated ORC summaries"

# ── Reports Zone ──────────────────────────────────────────
# JSON/CSV drift reports (MapReduce weekly job)
mkdirs "/satellite/reports/drift" \
    "TLE drift analysis reports — partitioned by week=YYYY-WW"

# Spark-generated HTML/JSON summary reports
mkdirs "/satellite/reports/daily" \
    "Daily Spark aggregation report exports"

# ── System Directories ────────────────────────────────────
# Spark Structured Streaming checkpoints
mkdirs "/satellite/checkpoints/enrichment" \
    "Checkpoint for orbit enrichment streaming job"

mkdirs "/satellite/checkpoints/anomaly" \
    "Checkpoint for anomaly detection streaming job"

mkdirs "/satellite/checkpoints/hdfs-positions" \
    "Checkpoint for HDFS position sink"

# Spark event logs (for history server)
mkdirs "/spark-logs" \
    "Spark application event logs"

# ── Permissions ───────────────────────────────────────────
echo ""
echo "Setting directory permissions..."
hdfs dfs -chmod -R 777 /satellite
hdfs dfs -chmod -R 777 /spark-logs
echo "✓ Permissions set"

# ── Verify Structure ──────────────────────────────────────
echo ""
echo "======================================================"
echo " HDFS Directory Tree"
echo "======================================================"
hdfs dfs -ls -R /satellite | awk '{print $NF}' | sort

echo ""
echo "======================================================"
echo " HDFS initialized successfully!"
echo "======================================================"
