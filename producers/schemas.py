"""
Pydantic message schemas for all Kafka topics.

Each schema corresponds to one Kafka topic and defines the canonical
message format for the entire pipeline.

Design Pattern: Schema Registry (enforced in-process via Pydantic v2).
All producers validate their output against these schemas before publishing.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Helpers ───────────────────────────────────────────────


def utc_now() -> str:
    """Return current UTC time as ISO-8601 string with Z suffix."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


# ── Topic: sat.position.raw ───────────────────────────────


class PositionMessage(BaseModel):
    """
    Schema for the sat.position.raw Kafka topic.

    Produced by: ISSProducer (source='open-notify')
                 N2YOProducer (source='n2yo')
    Consumed by: Spark Structured Streaming orbit_enrichment job.

    Both producers share this schema; optional fields are populated
    only by N2YO (which returns richer orbital data).
    """

    satellite_id: int
    satellite_name: str
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    altitude_km: Optional[float] = None         # Not available from Open-Notify
    azimuth: Optional[float] = None             # Degrees clockwise from North
    elevation: Optional[float] = None           # Degrees above observer horizon
    ra: Optional[float] = None                  # Right ascension (hours)
    dec: Optional[float] = None                 # Declination (degrees)
    eclipsed: Optional[bool] = None             # True if in Earth's shadow (N2YO only)
    timestamp: int                              # Unix epoch from the data source
    source: str                                 # "open-notify" | "n2yo"
    ingestion_time: str = Field(default_factory=utc_now)

    def kafka_key(self) -> str:
        """Partition key — all messages for one satellite go to the same partition."""
        return f"SAT-{self.satellite_id}"


# ── Topic: sat.tle.raw ────────────────────────────────────


class TLEMessage(BaseModel):
    """
    Schema for the sat.tle.raw Kafka topic.

    Produced by: TLEProducer (source='tle-api')
    Consumed by: MapReduce TLE drift analysis batch job.

    TLE data is published hourly; only changed TLEs are forwarded
    (change detection on tle_line1 epoch field).
    """

    satellite_id: int
    satellite_name: str
    tle_line1: str      # Standard 69-char TLE line 1
    tle_line2: str      # Standard 69-char TLE line 2
    epoch: Optional[str] = None     # Parsed from TLE line1 (ISO-8601)
    source: str = "tle-api"
    ingestion_time: str = Field(default_factory=utc_now)

    def kafka_key(self) -> str:
        return f"TLE-{self.satellite_id}"


# ── Topic: sat.events.raw ─────────────────────────────────


class SpaceWeatherEvent(BaseModel):
    """
    Schema for the sat.events.raw Kafka topic.

    Produced by: DONKIProducer (source='donki')
    Consumed by: Spark Structured Streaming anomaly detection job,
                 Airflow batch trigger DAG.

    Covers 6 NASA DONKI event types: CME, GST, FLR, SEP, IPS, HSS.
    The raw_data field preserves the full original API response for
    downstream jobs that need type-specific fields.
    """

    event_id: str                               # Unique ID from DONKI or derived hash
    event_type: str                             # CME | GST | FLR | SEP | IPS | HSS
    start_time: Optional[str] = None            # ISO-8601 event start
    peak_time: Optional[str] = None             # ISO-8601 peak intensity (FLR/CME)
    end_time: Optional[str] = None              # ISO-8601 event end
    source_location: Optional[str] = None       # Solar region (e.g., "N15W30")
    instruments: List[str] = Field(default_factory=list)
    linked_events: List[str] = Field(default_factory=list)
    severity: Optional[str] = None             # low | moderate | high | extreme
    note: Optional[str] = None
    raw_data: Dict[str, Any] = Field(default_factory=dict)
    source: str = "donki"
    ingestion_time: str = Field(default_factory=utc_now)

    def kafka_key(self) -> str:
        """Partition by event type so all CMEs go to one partition, etc."""
        return f"EVT-{self.event_type}"
