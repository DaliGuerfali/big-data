"""
TLE (Two-Line Element) parser utility.

Parses the standard two-line element set format used by NORAD/CelesTrak.
Returns a dict of all orbital elements as Python native types.

Reference: https://celestrak.org/NORAD/documentation/tle-fmt.php

Used by both the Hadoop MapReduce mapper and the Spark batch jobs.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional


def _checksum(line: str) -> int:
    """Compute the TLE line checksum (modulo 10)."""
    total = 0
    for ch in line[:-1]:
        if ch.isdigit():
            total += int(ch)
        elif ch == "-":
            total += 1
    return total % 10


def parse_tle(line1: str, line2: str, strict: bool = True) -> dict:
    """
    Parse TLE line1 and line2 into a dict of orbital elements.

    Args:
        line1:  TLE line 1 (69 chars)
        line2:  TLE line 2 (69 chars)
        strict: If True (default), raise ValueError on checksum mismatch.
                Set to False for testing with synthetic TLE data.

    Returns:
        {
            "norad_id":        int,
            "classification":  str,       # U / C / S
            "intl_designator": str,
            "epoch":           datetime,  # UTC
            "epoch_iso":       str,
            "bstar":           float,     # drag term (1/earth-radii)
            "mean_motion_dot": float,     # 1st deriv of mean motion (rev/day²)
            "inclination":     float,     # degrees
            "raan":            float,     # right ascension of ascending node (deg)
            "eccentricity":    float,
            "arg_perigee":     float,     # degrees
            "mean_anomaly":    float,     # degrees
            "mean_motion":     float,     # revolutions per day
            "rev_number":      int,
        }

    Raises:
        ValueError if lines are malformed or (when strict=True) checksums don't match.
    """
    line1 = line1.strip()
    line2 = line2.strip()

    if len(line1) != 69:
        raise ValueError(f"TLE line1 must be 69 chars, got {len(line1)}")
    if len(line2) != 69:
        raise ValueError(f"TLE line2 must be 69 chars, got {len(line2)}")

    if strict:
        if _checksum(line1) != int(line1[68]):
            raise ValueError(f"TLE line1 checksum mismatch (expected {_checksum(line1)}, got {line1[68]})")
        if _checksum(line2) != int(line2[68]):
            raise ValueError(f"TLE line2 checksum mismatch (expected {_checksum(line2)}, got {line2[68]})")

    # ── Line 1 ────────────────────────────────────────────────────────────────
    norad_id       = int(line1[2:7].strip())
    classification = line1[7].strip()
    intl_designator = line1[9:17].strip()

    # Epoch: two-digit year + day-of-year with fractional day
    epoch_year_2d = int(line1[18:20])
    epoch_year = (2000 + epoch_year_2d) if epoch_year_2d < 57 else (1900 + epoch_year_2d)
    epoch_day_frac = float(line1[20:32].strip())
    doy = int(epoch_day_frac)
    frac_day = epoch_day_frac - doy
    epoch_dt = datetime(epoch_year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1 + frac_day)

    # Mean motion first derivative (rev/day²), stored as .XXXXXXXX
    mm_dot_str = line1[33:43].strip()
    mean_motion_dot = float(mm_dot_str)

    # BSTAR drag term in packed notation: ±XXXXX±Y → ±0.XXXXX × 10^Y
    bstar_str = line1[53:61].strip()
    bstar = _parse_packed_float(bstar_str)

    # ── Line 2 ────────────────────────────────────────────────────────────────
    inclination  = float(line2[8:16].strip())
    raan         = float(line2[17:25].strip())
    eccentricity = float("0." + line2[26:33].strip())
    arg_perigee  = float(line2[34:42].strip())
    mean_anomaly = float(line2[43:51].strip())
    mean_motion  = float(line2[52:63].strip())
    rev_number   = int(line2[63:68].strip()) if line2[63:68].strip() else 0

    return {
        "norad_id":         norad_id,
        "classification":   classification,
        "intl_designator":  intl_designator,
        "epoch":            epoch_dt,
        "epoch_iso":        epoch_dt.isoformat().replace("+00:00", "Z"),
        "bstar":            bstar,
        "mean_motion_dot":  mean_motion_dot,
        "inclination":      inclination,
        "raan":             raan,
        "eccentricity":     eccentricity,
        "arg_perigee":      arg_perigee,
        "mean_anomaly":     mean_anomaly,
        "mean_motion":      mean_motion,
        "rev_number":       rev_number,
    }


def _parse_packed_float(s: str) -> float:
    """
    Parse a TLE packed decimal like " 12345-3" or "-12345-3".

    Format: [sign]XXXXXSY  where S is the sign of the exponent.
    Equivalent to ±0.XXXXX × 10^(±Y).
    """
    s = s.strip()
    if not s or s in ("-", "+", " "):
        return 0.0

    sign = -1.0 if s[0] == "-" else 1.0
    s = s.lstrip("+-").strip()

    # Find sign of exponent (last + or - that isn't the leading sign)
    for i in range(len(s) - 1, -1, -1):
        if s[i] in ("+", "-"):
            mantissa_str = s[:i]
            exp_sign = -1 if s[i] == "-" else 1
            exp_val  = int(s[i + 1:]) if s[i + 1:] else 0
            mantissa = float("0." + mantissa_str) if mantissa_str else 0.0
            return sign * mantissa * (10 ** (exp_sign * exp_val))

    return sign * float("0." + s)


def elements_to_csv(elements: dict) -> str:
    """Serialize parsed elements to a tab-separated string for MapReduce output."""
    return "\t".join([
        elements["epoch_iso"],
        str(elements["mean_motion"]),
        str(elements["eccentricity"]),
        str(elements["inclination"]),
        str(elements["raan"]),
        str(elements["arg_perigee"]),
        str(elements["mean_anomaly"]),
        str(elements["bstar"]),
    ])


def csv_to_elements(csv_str: str) -> dict:
    """Deserialize a tab-separated string back to elements dict (used in reducer)."""
    parts = csv_str.split("\t")
    if len(parts) != 8:
        raise ValueError(f"Expected 8 fields, got {len(parts)}: {csv_str!r}")

    epoch_str, mean_motion, eccentricity, inclination, raan, arg_perigee, mean_anomaly, bstar = parts

    epoch_dt = datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))
    return {
        "epoch":        epoch_dt,
        "epoch_iso":    epoch_str,
        "mean_motion":  float(mean_motion),
        "eccentricity": float(eccentricity),
        "inclination":  float(inclination),
        "raan":         float(raan),
        "arg_perigee":  float(arg_perigee),
        "mean_anomaly": float(mean_anomaly),
        "bstar":        float(bstar),
    }
