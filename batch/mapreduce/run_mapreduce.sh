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

# ── Run MapReduce via Python pipes (local mode) ───────────────────────────────
# Hadoop Streaming JAR 3.x rejects absolute -file paths (RunJar security check).
# Since mapreduce.framework.name=local we emulate Streaming with Python pipes —
# identical semantics: mapper | sort (shuffle) | reducer → HDFS output.

TMP_INPUT=$(mktemp /tmp/tle_input.XXXXXX)
TMP_OUTPUT=$(mktemp /tmp/tle_output.XXXXXX)

echo "[run_mapreduce] Pulling input from HDFS..."
hdfs dfs -cat "${HDFS_INPUT}/date=${YEAR}-*/*.json" 2>/dev/null > "${TMP_INPUT}" || \
hdfs dfs -cat "${HDFS_INPUT}/date=${YEAR}-*/*"      2>/dev/null > "${TMP_INPUT}" || true

INPUT_LINES=$(wc -l < "${TMP_INPUT}")
echo "[run_mapreduce] Input lines: ${INPUT_LINES}"

if [[ "${INPUT_LINES}" -eq 0 ]]; then
    echo "[run_mapreduce] WARNING: No input data for ${YEAR}-W${WEEK}. Writing empty report."
    echo "{\"warning\":\"no_input_data\",\"year\":\"${YEAR}\",\"week\":\"${WEEK}\"}" > "${TMP_OUTPUT}"
    EXIT_CODE=0
else
    echo "[run_mapreduce] Running mapper | sort | reducer..."
    PYTHONPATH="${MAPREDUCE_DIR}" python3 "${MAPREDUCE_DIR}/tle_drift_mapper.py" < "${TMP_INPUT}" \
        | sort \
        | PYTHONPATH="${MAPREDUCE_DIR}" python3 "${MAPREDUCE_DIR}/tle_drift_reducer.py" \
        > "${TMP_OUTPUT}"
    EXIT_CODE=$?
fi

rm -f "${TMP_INPUT}"

if [[ "${EXIT_CODE}" -eq 0 ]]; then
    echo "[run_mapreduce] Pipeline finished. Uploading to HDFS..."
    hdfs dfs -mkdir -p "${HDFS_OUTPUT}"
    hdfs dfs -put "${TMP_OUTPUT}" "${HDFS_OUTPUT}/part-00000"
    rm -f "${TMP_OUTPUT}"
    echo "[run_mapreduce] Job completed successfully."
    echo "[run_mapreduce] Results at: ${HDFS_OUTPUT}"
    echo "[run_mapreduce] Preview:"
    hdfs dfs -cat "${HDFS_OUTPUT}/part-00000" 2>/dev/null | head -5 || true
else
    rm -f "${TMP_OUTPUT}"
    echo "[run_mapreduce] ERROR: Pipeline failed with exit code ${EXIT_CODE}" >&2
    exit "${EXIT_CODE}"
fi
