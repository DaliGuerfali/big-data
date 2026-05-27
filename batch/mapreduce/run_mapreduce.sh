#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Weekly TLE Drift Analysis — Hadoop Streaming Job
#
# Usage:
#   ./run_mapreduce.sh [WEEK_NUMBER] [YEAR]
#
# Examples:
#   ./run_mapreduce.sh 3 2024        # Process week 3 of 2024
#   ./run_mapreduce.sh               # Auto-detect: current ISO week
#
# The script:
#   1. Resolves the input HDFS path for the given week's TLE data
#   2. Removes any existing output path (MapReduce won't overwrite)
#   3. Submits the Hadoop Streaming job
#   4. Prints a summary on completion
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────

YEAR="${2:-$(date -u +%Y)}"
WEEK="${1:-$(date -u +%V)}"          # ISO week number (01–53)

HDFS_BASE="/satellite"
HDFS_INPUT="${HDFS_BASE}/raw/tle"
HDFS_OUTPUT="${HDFS_BASE}/reports/drift/week=${YEAR}-$(printf '%02d' "${WEEK}")"

MAPREDUCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HADOOP_STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar"

# ── Resolve streaming jar (glob may match multiple versions) ─────────────────
STREAMING_JAR="$(ls ${HADOOP_STREAMING_JAR} 2>/dev/null | head -n 1)"
if [[ -z "${STREAMING_JAR}" ]]; then
    echo "[run_mapreduce] ERROR: Hadoop streaming jar not found at ${HADOOP_STREAMING_JAR}"
    exit 1
fi

echo "[run_mapreduce] ─────────────────────────────────────────────────────"
echo "[run_mapreduce] Year:        ${YEAR}"
echo "[run_mapreduce] Week:        ${WEEK}"
echo "[run_mapreduce] Input HDFS:  ${HDFS_INPUT}"
echo "[run_mapreduce] Output HDFS: ${HDFS_OUTPUT}"
echo "[run_mapreduce] Jar:         ${STREAMING_JAR}"
echo "[run_mapreduce] ─────────────────────────────────────────────────────"

# ── Remove existing output if present ────────────────────────────────────────
if hdfs dfs -test -d "${HDFS_OUTPUT}" 2>/dev/null; then
    echo "[run_mapreduce] Removing existing output: ${HDFS_OUTPUT}"
    hdfs dfs -rm -r "${HDFS_OUTPUT}"
fi

# ── Submit Hadoop Streaming job ───────────────────────────────────────────────
hadoop jar "${STREAMING_JAR}" \
    -input  "${HDFS_INPUT}/date=${YEAR}-*" \
    -output "${HDFS_OUTPUT}" \
    -mapper  "python3 tle_drift_mapper.py" \
    -reducer "python3 tle_drift_reducer.py" \
    -file    "${MAPREDUCE_DIR}/tle_drift_mapper.py" \
    -file    "${MAPREDUCE_DIR}/tle_drift_reducer.py" \
    -file    "${MAPREDUCE_DIR}/tle_parser.py" \
    -jobconf "mapreduce.job.name=TLE-Drift-Analysis-${YEAR}-W${WEEK}" \
    -jobconf "mapreduce.map.memory.mb=512" \
    -jobconf "mapreduce.reduce.memory.mb=512" \
    -jobconf "mapreduce.job.reduces=1"

EXIT_CODE=$?

if [[ "${EXIT_CODE}" -eq 0 ]]; then
    echo "[run_mapreduce] Job completed successfully."
    echo "[run_mapreduce] Results at: ${HDFS_OUTPUT}"
    echo "[run_mapreduce] Preview:"
    hdfs dfs -cat "${HDFS_OUTPUT}/part-00000" | head -5 || true
else
    echo "[run_mapreduce] ERROR: Job failed with exit code ${EXIT_CODE}" >&2
    exit "${EXIT_CODE}"
fi
