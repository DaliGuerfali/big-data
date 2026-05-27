#!/usr/bin/env python3
"""
Hadoop Streaming Reducer: TLE Drift Analysis

Design Pattern: MapReduce (Lambda Architecture - Batch Layer)

Input:  Sorted (satellite_id \\t csv_elements) pairs from the mapper
Output: One JSON drift analysis report per satellite to stdout

For each satellite, collects all TLE epochs in chronological order and
computes drift rates (change per day) between consecutive TLE snapshots.
Flags anomalous drift that may indicate orbital maneuvers or debris events.
"""

import sys
import json
from datetime import timezone

# tle_parser.py is distributed via -file flag in the hadoop streaming command
from tle_parser import csv_to_elements

# Thresholds for anomaly detection
MEAN_MOTION_DRIFT_THRESHOLD  = 0.001   # rev/day² — noticeable burn or decay
INCLINATION_DRIFT_THRESHOLD  = 0.01    # deg/day  — plane change maneuver
ECCENTRICITY_DRIFT_THRESHOLD = 0.0001  # per day  — orbit circularisation


def calculate_drift(elements_history: list) -> list:
    """
    Compute drift rates between consecutive TLE epochs.

    Returns a list of drift dicts, one per interval between adjacent TLEs.
    """
    if len(elements_history) < 2:
        return []

    elements_history.sort(key=lambda x: x["epoch"])
    drifts = []

    for i in range(1, len(elements_history)):
        prev = elements_history[i - 1]
        curr = elements_history[i]

        dt_seconds = (curr["epoch"] - prev["epoch"]).total_seconds()
        if dt_seconds <= 0:
            continue
        dt_days = dt_seconds / 86400.0

        # Inclination drift needs shortest-arc difference
        incl_delta = curr["inclination"] - prev["inclination"]

        drift = {
            "period_start":          prev["epoch_iso"],
            "period_end":            curr["epoch_iso"],
            "dt_days":               round(dt_days, 4),
            "mean_motion_drift":     round((curr["mean_motion"]  - prev["mean_motion"])  / dt_days, 6),
            "inclination_drift":     round(incl_delta / dt_days, 6),
            "eccentricity_drift":    round((curr["eccentricity"] - prev["eccentricity"]) / dt_days, 8),
            "raan_drift":            round((curr["raan"]         - prev["raan"])         / dt_days, 6),
            "arg_perigee_drift":     round((curr["arg_perigee"]  - prev["arg_perigee"]) / dt_days, 6),
            "bstar_drift":           round((curr["bstar"]        - prev["bstar"])        / dt_days, 10),
        }
        drifts.append(drift)

    return drifts


def detect_anomalies(drifts: list) -> list:
    """Return list of anomaly flag strings for any threshold exceeded."""
    flags = []
    for d in drifts:
        if abs(d["mean_motion_drift"]) > MEAN_MOTION_DRIFT_THRESHOLD:
            flags.append(f"MEAN_MOTION_MANEUVER at {d['period_end']}")
        if abs(d["inclination_drift"]) > INCLINATION_DRIFT_THRESHOLD:
            flags.append(f"INCLINATION_CHANGE at {d['period_end']}")
        if abs(d["eccentricity_drift"]) > ECCENTRICITY_DRIFT_THRESHOLD:
            flags.append(f"ECCENTRICITY_CHANGE at {d['period_end']}")
    return flags


def emit_result(sat_id: str, elements_history: list) -> None:
    """Compute and print the drift analysis JSON for one satellite."""
    if not elements_history:
        return

    drifts    = calculate_drift(elements_history)
    anomalies = detect_anomalies(drifts)

    sorted_history = sorted(elements_history, key=lambda x: x["epoch"])

    result = {
        "satellite_id": int(sat_id) if sat_id.isdigit() else sat_id,
        "analysis_period": {
            "start": sorted_history[0]["epoch_iso"],
            "end":   sorted_history[-1]["epoch_iso"],
        },
        "tle_count":        len(elements_history),
        "drift_analysis":   drifts,
        "anomaly_detected": len(anomalies) > 0,
        "anomaly_flags":    anomalies,
        "summary": {
            "mean_mean_motion":      round(
                sum(e["mean_motion"] for e in elements_history) / len(elements_history), 6
            ),
            "mean_eccentricity":     round(
                sum(e["eccentricity"] for e in elements_history) / len(elements_history), 8
            ),
            "mean_inclination":      round(
                sum(e["inclination"] for e in elements_history) / len(elements_history), 4
            ),
        },
    }
    print(json.dumps(result))


def main():
    current_sat      = None
    elements_history = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue

        parts = line.split("\t", 1)
        if len(parts) != 2:
            print(f"[reducer] Skipping malformed line: {line!r}", file=sys.stderr)
            continue

        sat_id, csv_str = parts

        try:
            elements = csv_to_elements(csv_str)
        except ValueError as exc:
            print(f"[reducer] Parse error for sat {sat_id}: {exc}", file=sys.stderr)
            continue

        if current_sat != sat_id:
            if current_sat is not None:
                emit_result(current_sat, elements_history)
            current_sat      = sat_id
            elements_history = []

        elements_history.append(elements)

    # Flush the last satellite
    if current_sat is not None:
        emit_result(current_sat, elements_history)


if __name__ == "__main__":
    main()
