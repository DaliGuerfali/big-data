#!/usr/bin/env python3
"""
Hadoop Streaming Mapper: TLE Drift Analysis

Design Pattern: MapReduce (Lambda Architecture - Batch Layer)

Input:  JSON lines from HDFS /satellite/raw/tle/  (one TLE record per line)
Output: (satellite_id \\t epoch,mean_motion,eccentricity,...) pairs

The reducer receives all records for the same satellite grouped together
(Hadoop sorts by key before sending to the reducer), so it can compute
consecutive drift rates between TLE epochs.
"""

import sys
import json

# tle_parser.py is distributed via -file flag in the hadoop streaming command
from tle_parser import parse_tle, elements_to_csv


def process_line(line: str) -> None:
    """Parse one JSON TLE record and emit (satellite_id, csv_elements)."""
    line = line.strip()
    if not line:
        return

    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        print(f"[mapper] Skipping malformed JSON: {exc}", file=sys.stderr)
        return

    sat_id   = record.get("satellite_id")
    tle_l1   = record.get("tle_line1", "").strip()
    tle_l2   = record.get("tle_line2", "").strip()

    if not sat_id or not tle_l1 or not tle_l2:
        print(f"[mapper] Skipping record with missing fields: {record}", file=sys.stderr)
        return

    try:
        elements = parse_tle(tle_l1, tle_l2, strict=False)
    except ValueError as exc:
        print(f"[mapper] TLE parse error for sat {sat_id}: {exc}", file=sys.stderr)
        return

    # Emit: satellite_id \t epoch_iso,mean_motion,eccentricity,...
    print(f"{sat_id}\t{elements_to_csv(elements)}")


def main():
    for line in sys.stdin:
        process_line(line)


if __name__ == "__main__":
    main()
